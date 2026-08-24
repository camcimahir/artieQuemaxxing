package main

import (
	"fmt"
	"time"

	"queuemaxxing/pkg/engine"
	"queuemaxxing/pkg/model"
)

func main() {
	fmt.Println("--- Starting Frankenstein Engine Sandbox ---")

	q, err := engine.NewFrankensteinQueue(model.ModeFIFO)
	if err != nil {
		panic(err)
	}
	realNow := time.Now()

	// 2. Create a message with a 5-second delay
	msg := model.Message{
		ID:          "msg-123",
		Payload:     "Hello from the future!",
		Priority:    1,
		EnqueuedAt:  realNow,
		AvailableAt: realNow.Add(5 * time.Second),
	}

	fmt.Printf("[0s] Pushing message. It becomes available at: %s\n", msg.AvailableAt.Format("15:04:05"))
	q.Push(msg)

	// 3. Attempt to Pop immediately (passing in the current time)
	_, foundNow := q.Pop(realNow)
	if !foundNow {
		fmt.Println("[0s] Attempted to Pop immediately: FAILED (Message is still invisible due to delay).")
	}

	// 4. Time Travel: Simulate the clock moving forward by 6 seconds
	futureTime := realNow.Add(6 * time.Second)
	fmt.Println("\n... Simulating 6 seconds passing ...")

	// 5. Attempt to Pop again, but pass the simulated future time to the engine
	poppedMsg, foundLater := q.Pop(futureTime)
	if foundLater {
		fmt.Printf("[+6s] Attempted to Pop: SUCCESS! Retrieved payload: '%s'\n", poppedMsg.Payload)
	} else {
		fmt.Println("[+6s] Attempted to Pop: FAILED (Something is wrong with the engine logic).")
	}
}
