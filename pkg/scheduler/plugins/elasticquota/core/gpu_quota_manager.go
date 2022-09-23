/*
Copyright 2022 The Koordinator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package core

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koordinator/apis/extension"
	"github.com/koordinator-sh/koordinator/pkg/util"
)

const (
	GPUResourceCardRatio = "gpu-card-ratio"
)

type EvictAssignInfo struct {
	quotaName   string
	assignedNum int64
}

type QuotaMeta struct {
	quotaName       string
	parentGroupName string
	oriMaxQuota     v1.ResourceList
	oriMinQuota     v1.ResourceList
}

type GpuQuotaManager struct {
	dataLock sync.RWMutex
	// gpuTypeDict records gpuTypes in cluster
	gpuTypeDict       sets.String
	groupQuotaManager *GroupQuotaManager
	// generalGpuTypeList helps judge whether the quotaGroups need to manage or not. if the quotaGroup's maxQuota
	// contains one of the generalGpuTypeList key, the quotaGroup need to manage.
	generalGpuTypeList []v1.ResourceName
	// record the originalQuotaValue
	quotaMetaMap           map[string]*QuotaMeta
	clusterGpuTypeTotalRes v1.ResourceList
}

func NewGpuQuotaManager(groupQuotaManager *GroupQuotaManager) *GpuQuotaManager {
	mgr := &GpuQuotaManager{
		gpuTypeDict:            sets.NewString(),
		groupQuotaManager:      groupQuotaManager,
		generalGpuTypeList:     make([]v1.ResourceName, 0),
		clusterGpuTypeTotalRes: v1.ResourceList{},
		quotaMetaMap:           make(map[string]*QuotaMeta),
	}
	mgr.generalGpuTypeList = append(mgr.generalGpuTypeList, GPUResourceCardRatio)

	return mgr
}

func (gpuMgr *GpuQuotaManager) Start() {
	// update the minQuota of the quotaGroup in each gpuTypeDimension
	go wait.Until(func() {
		gpuMgr.calGpuTypeMinQuotaFromGeneralGpu()
	}, time.Second*10, wait.NeverStop)

	klog.Infof("GpuQuotaManager start")
}

func (gpuMgr *GpuQuotaManager) UpdateClusterGpuTypeTotalRes(totalRes v1.ResourceList) {
	gpuMgr.dataLock.Lock()
	defer gpuMgr.dataLock.Unlock()

	gpuMgr.clusterGpuTypeTotalRes = totalRes.DeepCopy()
}

func (gpuMgr *GpuQuotaManager) AddGpuTypeDict(gpuTypeTmp string) {
	gpuMgr.dataLock.Lock()
	defer gpuMgr.dataLock.Unlock()

	gpuType := strings.ToLower(gpuTypeTmp)
	if !gpuMgr.gpuTypeDict.Has(gpuType) {
		gpuMgr.gpuTypeDict.Insert(gpuType)
		klog.Infof("AddGpuTypeDict Success:%v", gpuType)

		go gpuMgr.updateQuotaMetaWhenNewGpuTypeAdd()
	}
}

func (gpuMgr *GpuQuotaManager) updateQuotaMetaWhenNewGpuTypeAdd() {
	gpuMgr.groupQuotaManager.hierarchyUpdateLock.Lock()
	defer gpuMgr.groupQuotaManager.hierarchyUpdateLock.Unlock()

	quotaTree := gpuMgr.buildQuotaTree()
	gpuMgr.updateQuotaMetaWhenNewGpuTypeAddRecursive(quotaTree, extension.RootQuotaName)
}

func (gpuMgr *GpuQuotaManager) buildQuotaTree() map[string][]string {
	gpuMgr.dataLock.Lock()
	defer gpuMgr.dataLock.Unlock()

	return gpuMgr.buildQuotaTreeNoLock()
}

func (gpuMgr *GpuQuotaManager) buildQuotaTreeNoLock() map[string][]string {
	result := make(map[string][]string)
	for quotaName, quotaMeta := range gpuMgr.quotaMetaMap {
		parentName := quotaMeta.parentGroupName
		result[parentName] = append(result[parentName], quotaName)
	}

	for _, slice := range result {
		sort.Strings(slice)
	}
	return result
}

func (gpuMgr *GpuQuotaManager) updateQuotaMetaWhenNewGpuTypeAddRecursive(quotaTree map[string][]string, parentName string) {
	if quotaTree[parentName] == nil || len(quotaTree[parentName]) == 0 {
		return
	}

	for _, subQuotaName := range quotaTree[parentName] {
		oriQuota := gpuMgr.groupQuotaManager.getOriQuotaNoLock(subQuotaName)
		if oriQuota != nil {
			gpuMgr.groupQuotaManager.UpdateQuota(oriQuota, false)
		}
	}

	for _, subQuotaName := range quotaTree[parentName] {
		gpuMgr.updateQuotaMetaWhenNewGpuTypeAddRecursive(quotaTree, subQuotaName)
	}
}

func (gpuMgr *GpuQuotaManager) addOrUpdateQuotaMetaMap(quotaName, parentName string, maxQuota, minQuota v1.ResourceList) {
	if quotaMeta, ok := gpuMgr.quotaMetaMap[quotaName]; !ok {
		quotaMeta = &QuotaMeta{
			quotaName:       quotaName,
			parentGroupName: parentName,
			oriMaxQuota:     maxQuota,
			oriMinQuota:     minQuota,
		}
		gpuMgr.quotaMetaMap[quotaName] = quotaMeta
	} else {
		quotaMeta.parentGroupName = parentName
		quotaMeta.oriMaxQuota = maxQuota.DeepCopy()
		quotaMeta.oriMinQuota = minQuota.DeepCopy()
	}
}

func (gpuMgr *GpuQuotaManager) deleteQuotaMetaMap(quotaName string) {
	delete(gpuMgr.quotaMetaMap, quotaName)
}

// rebuildTree call this function
func (gpuMgr *GpuQuotaManager) updateQuotaMeta(quotaName, parentName string, newMaxQuota, newMinQuota, newSharedWeight v1.ResourceList) {
	klog.V(5).Infof("UpdateQuotaMeta begin, subQuotaNae:%v, maxQuota:%v, sharedRatio:%v",
		quotaName, newMaxQuota, newSharedWeight)

	gpuMgr.dataLock.Lock()
	defer gpuMgr.dataLock.Unlock()

	gpuMgr.addOrUpdateQuotaMetaMap(quotaName, parentName, newMaxQuota, newMinQuota)

	if !gpuMgr.hasGeneralGpuType(newMaxQuota) {
		return
	}

	maxQuotaKeyOnGeneralGpuType := gpuMgr.getGeneralGpuType(newMaxQuota)

	for _, gpuType := range gpuMgr.gpuTypeDict.List() {
		if !hasSpecificGpuTypeInResource(gpuType, newMaxQuota) {
			gpuTypeResName := buildSpecificGpuTypeResName(gpuType)

			newMaxQuota[gpuTypeResName] = newMaxQuota[maxQuotaKeyOnGeneralGpuType]
			newSharedWeight[gpuTypeResName] = newSharedWeight[maxQuotaKeyOnGeneralGpuType]
		}
	}

	klog.V(5).Infof("UpdateQuotaMeta end, subQuotaName:%v, maxQuota:%v, scaleRatio:%v",
		quotaName, newMaxQuota, newSharedWeight)

	quotaInfo := gpuMgr.groupQuotaManager.GetQuotaInfoByNameNoLock(quotaName)
	if quotaInfo != nil {
		gpuMgr.updateUsedQuotaNoLock(quotaInfo)
	}
}

func (gpuMgr *GpuQuotaManager) updateUsedQuotaNoLock(quotaInfo *QuotaInfo) v1.ResourceList {
	klog.V(5).Infof("updateUsedQuota begin, subQuotaName:%v, subQuotaName:%v, maxQuota:%v, usedQuota:%v, requestLimit:%v",
		quotaInfo.Name, quotaInfo.Name, gpuMgr.quotaMetaMap[quotaInfo.Name].oriMaxQuota, quotaInfo.CalculateInfo.Used, quotaInfo.CalculateInfo.AutoLimitRequest)

	if !gpuMgr.hasGeneralGpuType(gpuMgr.quotaMetaMap[quotaInfo.Name].oriMaxQuota) {
		return nil
	}

	maxQuotaKeyOnGeneralGpuType := gpuMgr.getGeneralGpuType(gpuMgr.quotaMetaMap[quotaInfo.Name].oriMaxQuota)

	// step1, update total general used quota
	totalUsed := int64(0)
	for _, gpuType := range gpuMgr.gpuTypeDict.List() {
		gpuTypeResName := buildSpecificGpuTypeResName(gpuType)
		totalUsed += quotaInfo.CalculateInfo.Used.Name(gpuTypeResName, resource.DecimalSI).Value()
	}

	// step2 update different gpu type request limit
	generalMaxQuota := gpuMgr.quotaMetaMap[quotaInfo.Name].oriMaxQuota[maxQuotaKeyOnGeneralGpuType]
	generalMaxQuotaValue := generalMaxQuota.Value()
	requestLimitValue := generalMaxQuotaValue - totalUsed
	// maxQuota decrease
	if requestLimitValue < 0 {
		requestLimitValue = 0
	}

	requestLimit := v1.ResourceList{}
	for _, gpuType := range gpuMgr.gpuTypeDict.List() {
		gpuTypeResName := buildSpecificGpuTypeResName(gpuType)
		gpuTypeUsed := quotaInfo.CalculateInfo.Used.Name(gpuTypeResName, resource.DecimalSI).Value()
		requestLimit[gpuTypeResName] = *resource.NewQuantity(gpuTypeUsed+requestLimitValue, resource.DecimalSI)
	}

	klog.V(5).Infof("UpdateUsedQuota end, subQuotaName:%v, subQuotaName:%v, maxQuota:%v, usedQuota:%v, requestLimit:%v",
		quotaInfo.Name, quotaInfo.Name, gpuMgr.quotaMetaMap[quotaInfo.Name].oriMaxQuota, quotaInfo.CalculateInfo.Used, requestLimit)

	gpuMgr.groupQuotaManager.updateGroupAutoLimitRequestNoLock(quotaInfo.Name, requestLimit)

	return requestLimit
}

func (gpuMgr *GpuQuotaManager) hasGeneralGpuType(res v1.ResourceList) bool {
	return gpuMgr.getGeneralGpuType(res) != ""
}

func (gpuMgr *GpuQuotaManager) getGeneralGpuType(res v1.ResourceList) v1.ResourceName {
	for _, gpuType := range gpuMgr.generalGpuTypeList {
		if _, exist := res[gpuType]; exist {
			return gpuType
		}
	}
	return ""
}

func buildSpecificGpuTypeResName(gpuModel string) v1.ResourceName {
	return v1.ResourceName(strings.ToLower(fmt.Sprintf("%s-%s", GPUResourceCardRatio, gpuModel)))
}

// hasSpecificGpuTypeInResource
func hasSpecificGpuTypeInResource(gpuType string, res v1.ResourceList) bool {
	for resName := range res {
		if strings.Contains(string(resName), gpuType) {
			return true
		}
	}
	return false
}

func (gpuMgr *GpuQuotaManager) deleteQuota(quotaName string) {
	gpuMgr.dataLock.Lock()
	defer gpuMgr.dataLock.Unlock()

	gpuMgr.deleteQuotaMetaMap(quotaName)
}

// calGpuTypeMinQuotaFromGeneralGpu is called by a background thread loop to determine the distribution  of the quotaGroup
// which has general gpu dimension on minQuota. The reason is minQuota has a strict check rule, children' sum <= parent,
// so when changing the meta of a quotaGroup, also need to get the information of other quotaGroups with same parent. As
// a result, there is a problem of getting lock. To solve the problem, our implementation is a background cycle loop.
func (gpuMgr *GpuQuotaManager) calGpuTypeMinQuotaFromGeneralGpu() {
	gpuMgr.groupQuotaManager.hierarchyUpdateLock.Lock()
	defer gpuMgr.groupQuotaManager.hierarchyUpdateLock.Unlock()

	gpuMgr.dataLock.Lock()
	defer gpuMgr.dataLock.Unlock()

	// recursive from rootNode
	quotaTree := gpuMgr.buildQuotaTreeNoLock()

	// get current minQuota of all quotaGroups
	currentMinQuotaMap := make(map[string]v1.ResourceList)
	for parentName, childNames := range quotaTree {
		quotaInfo := gpuMgr.groupQuotaManager.GetQuotaInfoByNameNoLock(parentName)
		if quotaInfo != nil {
			currentMinQuotaMap[parentName] = quotaInfo.getMin()
		}
		for _, childName := range childNames {
			quotaInfo = gpuMgr.groupQuotaManager.GetQuotaInfoByNameNoLock(childName)
			if quotaInfo != nil {
				currentMinQuotaMap[childName] = quotaInfo.getMin()
			}
		}
	}

	gpuMgr.calGpuTypeMinQuotaForGeneralGpuRecursive(quotaTree, currentMinQuotaMap, extension.RootQuotaName)
}

func (gpuMgr *GpuQuotaManager) calGpuTypeMinQuotaForGeneralGpuRecursive(quotaTree map[string][]string,
	currentMinQuotaMap map[string]v1.ResourceList, parentName string) {
	if quotaTree[parentName] == nil || len(quotaTree[parentName]) == 0 {
		return
	}

	// Traverse each child quota group of the to see whether roll back the values allocated in the previous
	// round or not, due to parent own general min reduction.
	gpuMgr.evictSelfOverMinQuota(quotaTree, currentMinQuotaMap, parentName)

	// Traverse each dimension of child minQuota to see if the sum of subMinQuota between siblings exceeds parMin
	// due to the automatically assigned minQuota
	gpuMgr.evictBrotherOverMinQuota(quotaTree, currentMinQuotaMap, parentName)

	gpuMgr.assignMinQuota(quotaTree, currentMinQuotaMap, parentName)

	for _, subQuotaName := range quotaTree[parentName] {
		quotaInfo := gpuMgr.groupQuotaManager.GetQuotaInfoByNameNoLock(subQuotaName)
		quotaInfo.setMinQuotaNoLock(currentMinQuotaMap[subQuotaName])
		gpuMgr.groupQuotaManager.updateMinQuotaNoLock(quotaInfo)
	}

	for _, subQuotaName := range quotaTree[parentName] {
		gpuMgr.calGpuTypeMinQuotaForGeneralGpuRecursive(quotaTree, currentMinQuotaMap, subQuotaName)
	}
}

func (gpuMgr *GpuQuotaManager) evictSelfOverMinQuota(quotaTree map[string][]string,
	currentMinQuotaMap map[string]v1.ResourceList, parentName string) {
	for _, subQuotaName := range quotaTree[parentName] {
		klog.V(5).Infof("EvictSelfOverMinQuota begin, parQuotaName:%v, subQuotaName:%v, oriMinQuota:%v, realMinQuota:%v",
			parentName, subQuotaName, gpuMgr.quotaMetaMap[subQuotaName].oriMinQuota, currentMinQuotaMap[subQuotaName])

		// only manage subQuotaGroups, whose configure generalMin
		oriSubMinQuota := gpuMgr.quotaMetaMap[subQuotaName].oriMaxQuota
		if gpuMgr.hasGeneralGpuType(oriSubMinQuota) {
			currentSubMinQuota := currentMinQuotaMap[subQuotaName]
			minQuotaKeyOnGeneralGpuType := gpuMgr.getGeneralGpuType(oriSubMinQuota)
			oriGeneralMinTotal := oriSubMinQuota.Name(minQuotaKeyOnGeneralGpuType, resource.DecimalSI).Value()

			// distinguish which quotaGroups are manually set externally by the user and which ones need to be automatically
			// assigned in this dimension, calculate the sum respectively.
			oriMinHasGpuTypeMinQuotaSum := int64(0)
			autoMinHasGpuTypeMinQuotaSum := int64(0)
			for _, gpuType := range gpuMgr.gpuTypeDict.List() {
				gpuTypeResName := buildSpecificGpuTypeResName(gpuType)
				if !hasSpecificGpuTypeInResource(string(gpuTypeResName), oriSubMinQuota) {
					autoMinHasGpuTypeMinQuotaSum += currentSubMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
				} else {
					oriMinHasGpuTypeMinQuotaSum += currentSubMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
				}
			}

			// if exceed the configuration oriGeneralMinTotal, need evict
			if autoMinHasGpuTypeMinQuotaSum+oriMinHasGpuTypeMinQuotaSum > oriGeneralMinTotal {
				totalEvictCount := autoMinHasGpuTypeMinQuotaSum + oriMinHasGpuTypeMinQuotaSum - oriGeneralMinTotal

				for _, gpuType := range gpuMgr.gpuTypeDict.List() {
					gpuTypeResName := buildSpecificGpuTypeResName(gpuType)
					// evict the quotaGroups automatically assigned in this dimension
					if !hasSpecificGpuTypeInResource(string(gpuTypeResName), oriSubMinQuota) {
						canEvictCount := currentSubMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
						toEvictCount := util.MinInt64(totalEvictCount, canEvictCount)
						totalEvictCount -= toEvictCount
						currentSubMinQuota[gpuTypeResName] = *resource.NewQuantity(canEvictCount-toEvictCount, resource.DecimalSI)
						if totalEvictCount <= 0 {
							break
						}
					}
				}
			}
		}
		klog.V(5).Infof("EvictSelfOverMinQuota end, parQuotaName:%v, subQuotaName:%v, oriMinQuota:%v, realMinQuota:%v",
			parentName, subQuotaName, oriSubMinQuota, currentMinQuotaMap[subQuotaName])
	}
}

func (gpuMgr *GpuQuotaManager) evictBrotherOverMinQuota(quotaTree map[string][]string,
	currentMinQuotaMap map[string]v1.ResourceList, parentName string) {
	for _, subQuotaName := range quotaTree[parentName] {
		klog.V(5).Infof("EvictBrotherOverMinQuota begin, parQuotaName:%v, subQuotaName:%v, oriMinQuota:%v, realMinQuota:%v",
			parentName, subQuotaName, gpuMgr.quotaMetaMap[subQuotaName].oriMinQuota, currentMinQuotaMap[subQuotaName])
	}

	// traverse each gpuType dimension
	for _, gpuType := range gpuMgr.gpuTypeDict.List() {
		gpuTypeResName := buildSpecificGpuTypeResName(gpuType)

		// get parMinQuota in this gpuType
		parentMinQuotaValue := int64(0)
		if parentName == extension.RootQuotaName {
			parentMinQuotaValue = gpuMgr.clusterGpuTypeTotalRes.Name(gpuTypeResName, resource.DecimalSI).Value()
		} else {
			parentMinQuota := currentMinQuotaMap[parentName]
			parentMinQuotaValue = parentMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
		}

		// distinguish which quota groups are manually set externally by the user and which ones need to
		// be automatically calculated in this dimension
		oriMinHasGpuTypeMinQuotaSum, autoMinHasGpuTypeMinQuotaSum := int64(0), int64(0)
		autoAssignedGpuTypeQuotaGroups := make([]*EvictAssignInfo, 0)
		for _, subQuotaName := range quotaTree[parentName] {
			oriSubMinQuota := gpuMgr.quotaMetaMap[subQuotaName].oriMinQuota
			currentSubMinQuota := currentMinQuotaMap[subQuotaName]
			// the quotaGroup manually sets the minQuota in this gpuType
			if hasSpecificGpuTypeInResource(string(gpuTypeResName), oriSubMinQuota) {
				oriMinHasGpuTypeMinQuotaSum += currentSubMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
			} else {
				autoMinHasGpuTypeMinQuotaSum += currentSubMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
				autoAssignedGpuTypeQuotaGroups = append(autoAssignedGpuTypeQuotaGroups,
					&EvictAssignInfo{
						quotaName:   subQuotaName,
						assignedNum: currentSubMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value(),
					})
			}
		}

		// if minQuota sum of the manually and autoAssigned is larger than parentMinQuota, need evict
		// the autoAssigned quotaGroups in this dimension
		if oriMinHasGpuTypeMinQuotaSum+autoMinHasGpuTypeMinQuotaSum > parentMinQuotaValue {
			totalEvictCount := oriMinHasGpuTypeMinQuotaSum + autoMinHasGpuTypeMinQuotaSum - parentMinQuotaValue
			for _, evictAssignInfo := range autoAssignedGpuTypeQuotaGroups {
				toEvictCount := util.MinInt64(totalEvictCount, evictAssignInfo.assignedNum)
				totalEvictCount -= toEvictCount
				currentMinQuotaMap[evictAssignInfo.quotaName][gpuTypeResName] = *resource.NewQuantity(
					evictAssignInfo.assignedNum-toEvictCount, resource.DecimalSI)
				if totalEvictCount <= 0 {
					break
				}
			}
		}
	}

	for _, subQuotaName := range quotaTree[parentName] {
		klog.V(5).Infof("EvictBrotherOverMinQuota end, parQuotaName:%v, subQuotaName:%v, oriMinQuota:%v, realMinQuota:%v",
			parentName, subQuotaName, gpuMgr.quotaMetaMap[subQuotaName].oriMinQuota, currentMinQuotaMap[subQuotaName])
	}
}

func (gpuMgr *GpuQuotaManager) assignMinQuota(quotaTree map[string][]string,
	currentMinQuotaMap map[string]v1.ResourceList, parentName string) {
	for _, subQuotaName := range quotaTree[parentName] {
		klog.V(5).Infof("AssignMinQuota begin, parQuotaName:%v, subQuotaName:%v, oriMinQuota:%v, realMinQuota:%v",
			parentName, subQuotaName, gpuMgr.quotaMetaMap[subQuotaName].oriMinQuota, currentMinQuotaMap[subQuotaName])
	}

	for _, gpuType := range gpuMgr.gpuTypeDict.List() {
		gpuTypeResName := buildSpecificGpuTypeResName(gpuType)

		// get parentMinQuota's value in this gpuTypeResName
		parentMinQuotaValue := int64(0)
		if parentName == extension.RootQuotaName {
			parentMinQuotaValue = gpuMgr.clusterGpuTypeTotalRes.Name(gpuTypeResName, resource.DecimalSI).Value()
		} else {
			parentMinQuota := currentMinQuotaMap[parentName]
			parentMinQuotaValue = parentMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
		}

		// distinguish which quotaGroups are manually set externally by the use and which ones need to be automatically
		// assigned in this dimension.
		oriMinHasGpuTypeMinQuotaSum, autoMinHasGpuTypeMinQuotaSum := int64(0), int64(0)
		autoAssignedGpuTypeQuotaGroups := make([]string, 0)
		for _, subQuotaName := range quotaTree[parentName] {
			subOriMinQuota := gpuMgr.quotaMetaMap[subQuotaName].oriMinQuota
			subCurrentMinQuota := currentMinQuotaMap[subQuotaName]
			// the quotaGroup manually sets the minQuota in this gpuType
			if hasSpecificGpuTypeInResource(string(gpuTypeResName), subOriMinQuota) {
				oriMinHasGpuTypeMinQuotaSum += subCurrentMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
			} else {
				autoMinHasGpuTypeMinQuotaSum += subCurrentMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
				autoAssignedGpuTypeQuotaGroups = append(autoAssignedGpuTypeQuotaGroups, subQuotaName)
			}
		}

		// if minQuota sum of the manually and autoAssigned is less than parentMinQuota in this dimension, need assign
		// the left to the autoAssigned quotaGroups
		if oriMinHasGpuTypeMinQuotaSum+autoMinHasGpuTypeMinQuotaSum < parentMinQuotaValue {
			parentCanAssignCount := parentMinQuotaValue - oriMinHasGpuTypeMinQuotaSum - autoMinHasGpuTypeMinQuotaSum

			for _, subQuotaName := range autoAssignedGpuTypeQuotaGroups {
				subOriMinQuota := gpuMgr.quotaMetaMap[subQuotaName].oriMinQuota
				subCurrentMinQuota := currentMinQuotaMap[subQuotaName]
				if parentCanAssignCount <= 0 {
					break
				}

				// no need to assign in subQuotaGroup which doesn't have generalGpuType
				if !gpuMgr.hasGeneralGpuType(subOriMinQuota) {
					continue
				}

				// get the subGroup's general min configuration
				minQuotaKeyOnGeneralGpuType := gpuMgr.getGeneralGpuType(subOriMinQuota)
				subOriGeneralMin := subOriMinQuota.Name(minQuotaKeyOnGeneralGpuType, resource.DecimalSI).Value()

				subOriHasGpuTypeMinQuotaSum, subAutoGpuTypeMinQuotaSum := int64(0), int64(0)
				for _, gpuTypeTmp := range gpuMgr.gpuTypeDict.List() {
					gpuTypeResNameTmp := buildSpecificGpuTypeResName(gpuTypeTmp)
					// the quotaGroup manually configures the min of this gpuType dimension
					if hasSpecificGpuTypeInResource(string(gpuTypeResNameTmp), subOriMinQuota) {
						subOriHasGpuTypeMinQuotaSum += subCurrentMinQuota.Name(gpuTypeResNameTmp, resource.DecimalSI).Value()
					} else {
						subAutoGpuTypeMinQuotaSum += subCurrentMinQuota.Name(gpuTypeResNameTmp, resource.DecimalSI).Value()
					}
				}

				subCanAssignCount := subOriGeneralMin - subOriHasGpuTypeMinQuotaSum - subAutoGpuTypeMinQuotaSum
				if subCanAssignCount <= 0 {
					continue
				}

				canAssignCount := util.MinInt64(parentCanAssignCount, subCanAssignCount)
				parentCanAssignCount -= canAssignCount

				currentValue := subCurrentMinQuota.Name(gpuTypeResName, resource.DecimalSI).Value()
				subCurrentMinQuota[gpuTypeResName] = *resource.NewQuantity(currentValue+canAssignCount, resource.DecimalSI)
			}
		}
	}
	for _, subQuotaName := range quotaTree[parentName] {
		klog.V(5).Infof("AssignMinQuota end, parQuotaName:%v, subQuotaName:%v, oriMinQuota:%v, realMinQuota:%v",
			parentName, subQuotaName, gpuMgr.quotaMetaMap[subQuotaName].oriMinQuota, currentMinQuotaMap[subQuotaName])
	}
}
