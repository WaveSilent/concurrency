package main

import (
	"concurrency/miner"
	"concurrency/postman"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var mtx sync.Mutex

func main() {
	var coal atomic.Int64
	var mails []string

	minerContext, minerCancel := context.WithCancel(context.Background())
	postmanContext, postmanCancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(3 * time.Second)
		fmt.Println("------->>Рабочий день шахтеров окончен")
		minerCancel()
	}()

	go func() {
		time.Sleep(6 * time.Second)
		fmt.Println("------->>Рабочий день почтальонов окончен")
		postmanCancel()
	}()

	coalTransferPoint := miner.MinerPool(minerContext, 100)
	postmanTransferPoint := postman.PostmanPool(postmanContext, 100)

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range coalTransferPoint {
			coal.Add(int64(v))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for v := range postmanTransferPoint {
			mtx.Lock()
			mails = append(mails, v)
			mtx.Unlock()
		}
	}()

	wg.Wait()

	fmt.Println("Суммарно добытый уголь:", coal.Load())

	mtx.Lock()
	fmt.Println("Суммарное кол-во писем:", len(mails))
	mtx.Unlock()

}
