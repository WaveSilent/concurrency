package miner

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func Miner(
	wg *sync.WaitGroup,
	ctx context.Context,
	transferPoint chan<- int,
	n int, power int,
) {
	defer wg.Done()

	for {
		fmt.Println("Я шахтер номер:", n, "начал добывать уголь")
		select {
		case <-ctx.Done():
			fmt.Println("Я шахтер номер:", n, "мой рабочий день закончен")
			return
		case <-time.After(1 * time.Second):
			fmt.Println("Я шахтер номер:", n, "добыл уголь")
		}

		select {
		case <-ctx.Done():
			fmt.Println("Я шахтер номер:", n, "мой рабочий день закончен")
			return
		case transferPoint <- power:
			fmt.Println("Я шахтер номер:", n, "передал уголь:", power)
		}
	}
}

func MinerPool(ctx context.Context, minerCount int) <-chan int {
	wg := &sync.WaitGroup{}
	coalTransferPoint := make(chan int)

	for i := 1; i <= minerCount; i++ {
		wg.Add(1)
		go Miner(wg, ctx, coalTransferPoint, i, i*10)
	}

	go func() {
		wg.Wait()
		close(coalTransferPoint)
	}()

	return coalTransferPoint
}
