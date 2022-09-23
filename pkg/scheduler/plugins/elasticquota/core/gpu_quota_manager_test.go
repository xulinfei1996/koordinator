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
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestGpuQuotaManager_TryAddGpuTypeDict(t *testing.T) {
	gqm := NewGroupQuotaManager4Test()
	gpuMgr := gqm.gpuQuotaManager

	assert.Equal(t, 0, len(gpuMgr.gpuTypeDict))

	gpuMgr.AddGpuTypeDict("V100")
	assert.Equal(t, 1, len(gpuMgr.gpuTypeDict))

	gpuMgr.AddGpuTypeDict("V100")
	assert.Equal(t, 1, len(gpuMgr.gpuTypeDict))

	gpuMgr.AddGpuTypeDict("A100")
	assert.Equal(t, 2, len(gpuMgr.gpuTypeDict))
}

func TestGpuQuotaManager_evictBrotherOverMinQuota(t *testing.T) {
	setLoglevel("5")
	v100GpuName := v1.ResourceName(GPUResourceCardRatio + "-v100")
	p100GpuName := v1.ResourceName(GPUResourceCardRatio + "-p100")
	a100GpuName := v1.ResourceName(GPUResourceCardRatio + "-a100")
	maxQuota := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 1000).Obj()
	minQuota := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).Obj()
	totalRes := MakeResourceList().Resource(v100GpuName, 100).
		Resource(p100GpuName, 100).Resource(a100GpuName, 100).Obj()
	{
		gqm := NewGroupQuotaManager4Test()
		gpuMgr := gqm.gpuQuotaManager
		gpuMgr.AddGpuTypeDict("v100")
		gpuMgr.AddGpuTypeDict("p100")
		gpuMgr.AddGpuTypeDict("a100")
		gpuMgr.updateQuotaMeta("2", "root", maxQuota, minQuota, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("3", "root", maxQuota, minQuota, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("4", "root", maxQuota, minQuota, v1.ResourceList{})
		quotaTree := make(map[string][]string)
		quotaTree["root"] = append(quotaTree["root"], "2")
		quotaTree["root"] = append(quotaTree["root"], "3")
		quotaTree["root"] = append(quotaTree["root"], "4")

		currentMinQuotaMap := make(map[string]v1.ResourceList)
		modifyQuota := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 50).Resource(a100GpuName, 50).Obj()
		currentMinQuotaMap["2"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 40).Resource(a100GpuName, 40).Obj()
		currentMinQuotaMap["3"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 40).Resource(a100GpuName, 40).Obj()
		currentMinQuotaMap["4"] = modifyQuota.DeepCopy()

		gpuMgr.UpdateClusterGpuTypeTotalRes(totalRes)
		gpuMgr.evictBrotherOverMinQuota(quotaTree, currentMinQuotaMap, "root")

		assert.Equal(t, currentMinQuotaMap["2"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 20).
			Resource(p100GpuName, 20).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["3"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 40).
			Resource(p100GpuName, 40).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["4"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 40).
			Resource(p100GpuName, 40).Resource(GPUResourceCardRatio, 100).Obj())
	}
	{
		gqm := NewGroupQuotaManager4Test()
		gpuMgr := gqm.gpuQuotaManager
		gpuMgr.AddGpuTypeDict("v100")
		gpuMgr.AddGpuTypeDict("p100")
		gpuMgr.AddGpuTypeDict("a100")
		gpuMgr.updateQuotaMeta("2", "root", maxQuota, minQuota, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("3", "root", maxQuota, minQuota, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("4", "root", maxQuota, minQuota, v1.ResourceList{})
		quotaTree := make(map[string][]string)
		quotaTree["20"] = append(quotaTree["20"], "2")
		quotaTree["20"] = append(quotaTree["20"], "3")
		quotaTree["20"] = append(quotaTree["20"], "4")

		currentMinQuotaMap := make(map[string]v1.ResourceList)
		modifyQuota := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 50).Resource(a100GpuName, 50).Obj()
		currentMinQuotaMap["2"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 40).Resource(a100GpuName, 40).Obj()
		currentMinQuotaMap["3"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 40).Resource(a100GpuName, 40).Obj()
		currentMinQuotaMap["4"] = modifyQuota.DeepCopy()

		currentMinQuotaMap["20"] = totalRes.DeepCopy()
		gpuMgr.evictBrotherOverMinQuota(quotaTree, currentMinQuotaMap, "20")

		assert.Equal(t, currentMinQuotaMap["2"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 20).
			Resource(p100GpuName, 20).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["3"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 40).
			Resource(p100GpuName, 40).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["4"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 40).
			Resource(p100GpuName, 40).Resource(GPUResourceCardRatio, 100).Obj())
	}
}

func TestGpuQuotaManager_assignMinQuota(t *testing.T) {
	setLoglevel("5")
	v100GpuName := v1.ResourceName(GPUResourceCardRatio + "-v100")
	p100GpuName := v1.ResourceName(GPUResourceCardRatio + "-p100")
	a100GpuName := v1.ResourceName(GPUResourceCardRatio + "-a100")
	maxQuota := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 1000).Obj()
	minQuota1 := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
		Resource(p100GpuName, 40).Resource(a100GpuName, 30).Obj()
	minQuota2 := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).Obj()

	quotaTree := make(map[string][]string)
	quotaTree["root"] = append(quotaTree["root"], "2")
	quotaTree["root"] = append(quotaTree["root"], "3")
	quotaTree["root"] = append(quotaTree["root"], "4")
	{
		gqm := NewGroupQuotaManager4Test()
		gpuMgr := gqm.gpuQuotaManager
		gpuMgr.AddGpuTypeDict("V100")
		gpuMgr.AddGpuTypeDict("P100")
		gpuMgr.AddGpuTypeDict("A100")

		gpuMgr.updateQuotaMeta("2", "root", maxQuota, minQuota1, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("3", "root", maxQuota, minQuota2, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("4", "root", maxQuota, minQuota2, v1.ResourceList{})

		currentMinQuotaMap := make(map[string]v1.ResourceList)
		modifyQuota := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 40).Resource(a100GpuName, 30).Obj()
		currentMinQuotaMap["2"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 10).Resource(a100GpuName, 20).Obj()
		currentMinQuotaMap["3"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 20).Resource(a100GpuName, 10).Obj()
		currentMinQuotaMap["4"] = modifyQuota.DeepCopy()

		totalRes := MakeResourceList().Resource(v100GpuName, 100).Resource(p100GpuName, 100).
			Resource(a100GpuName, 100).Obj()
		gpuMgr.UpdateClusterGpuTypeTotalRes(totalRes)
		gpuMgr.assignMinQuota(quotaTree, currentMinQuotaMap, "root")
		assert.Equal(t, currentMinQuotaMap["2"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 30).
			Resource(p100GpuName, 40).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["3"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 40).
			Resource(p100GpuName, 10).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["4"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 30).
			Resource(p100GpuName, 20).Resource(GPUResourceCardRatio, 100).Obj())
	}
	{
		gqm := NewGroupQuotaManager4Test()
		gpuMgr := gqm.gpuQuotaManager
		gpuMgr.AddGpuTypeDict("v100")
		gpuMgr.AddGpuTypeDict("p100")
		gpuMgr.AddGpuTypeDict("a100")

		gpuMgr.updateQuotaMeta("2", "20", maxQuota, minQuota1, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("3", "20", maxQuota, minQuota2, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("4", "20", maxQuota, minQuota2, v1.ResourceList{})

		currentMinQuotaMap := make(map[string]v1.ResourceList)
		modifyQuota := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 40).Resource(a100GpuName, 30).Obj()
		currentMinQuotaMap["2"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 10).Resource(a100GpuName, 20).Obj()
		currentMinQuotaMap["3"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 20).Resource(a100GpuName, 10).Obj()
		currentMinQuotaMap["4"] = modifyQuota.DeepCopy()

		totalRes := MakeResourceList().Resource(v100GpuName, 100).Resource(p100GpuName, 100).
			Resource(a100GpuName, 100).Obj()
		currentMinQuotaMap["20"] = totalRes.DeepCopy()
		quotaTree["20"] = append(quotaTree["20"], "2")
		quotaTree["20"] = append(quotaTree["20"], "3")
		quotaTree["20"] = append(quotaTree["20"], "4")
		gpuMgr.assignMinQuota(quotaTree, currentMinQuotaMap, "20")
		assert.Equal(t, currentMinQuotaMap["2"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 30).
			Resource(p100GpuName, 40).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["3"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 40).
			Resource(p100GpuName, 10).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["4"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 30).
			Resource(p100GpuName, 20).Resource(GPUResourceCardRatio, 100).Obj())
	}
	{
		gqm := NewGroupQuotaManager4Test()
		gpuMgr := gqm.gpuQuotaManager
		gpuMgr.AddGpuTypeDict("V100")
		gpuMgr.AddGpuTypeDict("P100")
		gpuMgr.AddGpuTypeDict("A100")

		delete(minQuota2, GPUResourceCardRatio)

		gpuMgr.updateQuotaMeta("2", "root", maxQuota, minQuota1, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("3", "root", maxQuota, minQuota2, v1.ResourceList{})
		gpuMgr.updateQuotaMeta("4", "root", maxQuota, minQuota2, v1.ResourceList{})

		currentMinQuotaMap := make(map[string]v1.ResourceList)
		modifyQuota := MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 40).Resource(a100GpuName, 30).Obj()
		currentMinQuotaMap["2"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 10).Resource(a100GpuName, 20).Obj()
		currentMinQuotaMap["3"] = modifyQuota.DeepCopy()

		modifyQuota = MakeResourceList().Resource(GPUResourceCardRatio, 100).Resource(v100GpuName, 50).
			Resource(p100GpuName, 20).Resource(a100GpuName, 10).Obj()
		currentMinQuotaMap["4"] = modifyQuota.DeepCopy()
		totalRes := MakeResourceList().Resource(v100GpuName, 100).Resource(p100GpuName, 100).
			Resource(a100GpuName, 100).Obj()
		gpuMgr.UpdateClusterGpuTypeTotalRes(totalRes)
		gpuMgr.assignMinQuota(quotaTree, currentMinQuotaMap, "root")
		assert.Equal(t, currentMinQuotaMap["2"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 30).
			Resource(p100GpuName, 40).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["3"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 20).
			Resource(p100GpuName, 10).Resource(GPUResourceCardRatio, 100).Obj())
		assert.Equal(t, currentMinQuotaMap["4"], MakeResourceList().Resource(v100GpuName, 50).Resource(a100GpuName, 10).
			Resource(p100GpuName, 20).Resource(GPUResourceCardRatio, 100).Obj())
	}
}

type resourceWrapper struct{ v1.ResourceList }

func MakeResourceList() *resourceWrapper {
	return &resourceWrapper{v1.ResourceList{}}
}

func (r *resourceWrapper) Resource(name v1.ResourceName, val int64) *resourceWrapper {
	r.ResourceList[name] = *resource.NewQuantity(val, resource.DecimalSI)
	return r
}

func (r *resourceWrapper) Obj() v1.ResourceList {
	return r.ResourceList
}
