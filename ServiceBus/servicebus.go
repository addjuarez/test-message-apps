package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jamiealquiza/tachymeter"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

var send = map[string]time.Time{}
var recieve = map[string]time.Time{}

var serviceBusConnectString = os.Getenv("SB_CS")
var sbQueue = os.Getenv("SB_QUEUE_NAME")

func main() {
	ctx := context.Background()
	go produce(ctx)
	go consume(ctx)
	consume(ctx)
}

func produce(ctx context.Context) {
	client, err := azservicebus.NewClientFromConnectionString(sbQueue, nil)

	if err != nil {
		panic(err)
	}

	sender, err := client.NewSender(serviceBusConnectString, nil)
	if err != nil {
		panic(err)
	}
	defer sender.Close(context.TODO())
	time.Sleep(10 * time.Second)
	for {

		for i := 0; i < 10000; i++ {
			sbMessage := &azservicebus.Message{
				Body: []byte(strconv.Itoa(i)),
			}
			send[strconv.Itoa(i)] = time.Now()

			err = sender.SendMessage(context.TODO(), sbMessage, nil)
			if err != nil {
				panic(err)
			}
			time.Sleep(time.Millisecond * 10)

		}
		time.Sleep(10 * time.Second)
		fmt.Println("Sent: ", len(send), " Recieved: ", len(recieve))
		t := tachymeter.New(&tachymeter.Config{Size: 10000})

		for k, v := range recieve {
			diff := v.Sub(send[k])
			t.AddTime(diff)
		}
		fmt.Println(t.Calc())

		time.Sleep(60 * time.Second)
		for k := range send {
			delete(send, k)
		}
		for k := range recieve {
			delete(recieve, k)
		}
	}
}

func consume(ctx context.Context) {
	client, err := azservicebus.NewClientFromConnectionString(serviceBusConnectString, nil)
	if err != nil {
		panic(err)
	}
	receiver, err := client.NewReceiverForQueue(sbQueue, nil)
	if err != nil {
		panic(err)
	}

	for {
		rctx, cancel := context.WithTimeout(context.TODO(), 60*time.Second)
		defer cancel()

		messages, err := receiver.ReceiveMessages(rctx,
			1,
			nil,
		)
		ts := time.Now()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			fmt.Print(err)
		}

		for _, message := range messages {
			//var body []byte = message.Body
			recieve[string(message.Body)] = ts
			go complete(client, receiver, message) // mark message as completed
			//fmt.Printf("Message received with body: %s\n", string(body))

		}
	}
}

func complete(client *azservicebus.Client, receiver *azservicebus.Receiver, message *azservicebus.ReceivedMessage) {
	err := receiver.CompleteMessage(context.TODO(), message, nil)

	if err != nil {
		var sbErr *azservicebus.Error

		if errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeLockLost {
			fmt.Printf("Message lock expired\n")
		}

		panic(err)
	}

	//fmt.Printf("Received and completed the message\n")
}
