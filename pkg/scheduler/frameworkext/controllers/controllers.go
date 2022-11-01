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

package controllers

import (
	"github.com/koordinator-sh/koordinator/pkg/descheduler/framework"
)

type ControllersMap struct {
	controllers map[string]Controller
}

func NewControllersMap() *ControllersMap {
	return &ControllersMap{
		controllers: make(map[string]Controller),
	}
}

func (cm ControllersMap) RegisterControllers(plugin framework.Plugin) {
	if controllerProvider, ok := plugin.(ControllerProvider); ok {
		controllerProvider.RegisterControllers(cm)
	}
}

func (cm ControllersMap) RegisterController(name string, controller Controller) {
	cm.controllers[name] = controller
}

func (cm ControllersMap) Start() {
	for _, controller := range cm.controllers {
		controller.Start()
	}
}
