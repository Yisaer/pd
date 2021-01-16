package main

import (
	"fmt"
	"github.com/tikv/pd/pkg/copysets"
)

func main() {

	initialCount := 100
	nodesID := make([]uint64, 0, 0)
	for i := 1; i <= initialCount; i++ {
		nodesID = append(nodesID, uint64(i))
	}
	cm := copysets.NewCopysetsManager(3, 6, nodesID)
	css := cm.GenerateCopySets()
	for _, cs := range css {
		fmt.Println(cs.Sign())
	}

	//cs := cm.GenerateCopySets()
	//printInfo(initialCount, len(cs))
	//addNodeCount := 5000
	//for i := initialCount + 1; i <= initialCount+addNodeCount; i++ {
	//	cm.AddNode(uint64(i))
	//	printInfo(i, len(cm.GenerateCopySets()))
	//}
}

func printInfo(nodes int, copysetCount int) {
	x := fmt.Sprintf("node Size:%v, copyset cnt:%v", nodes, copysetCount)
	fmt.Println(x)
}
