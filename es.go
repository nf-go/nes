// Copyright 2020 The nfgo Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nes

import (
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"nfgo.ga/nfgo/nlog"
)

// ESConfig -
type ESConfig struct {
	Addrs    []string `yaml:"addrs"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

// NewESAPI -
func NewESAPI(config *ESConfig) (*esapi.API, error) {
	c := es.Config{
		Addresses: config.Addrs,
		Username:  config.Username,
		Password:  config.Password,
	}
	client, err := es.NewClient(c)
	if err != nil {
		return nil, err
	}
	return client.API, nil
}

// MustNewESAPI -
func MustNewESAPI(config *ESConfig) *esapi.API {
	api, err := NewESAPI(config)
	if err != nil {
		nlog.Fatal("fail to create esapi: ", err)
	}
	return api
}
