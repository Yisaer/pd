// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package copysets

import "math/rand"

func selectN(n int, ids []uint64) []uint64 {
	if n > len(ids) {
		panic("selectN panic")
	}
	s := make([]uint64, 0, n)
	p := rand.Perm(len(ids))
	for i := 0; i < n; i++ {
		s = append(s, ids[p[i]])
	}
	return s
}
