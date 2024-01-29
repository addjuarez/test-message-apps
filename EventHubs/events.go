package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"errors"

	"github.com/jamiealquiza/tachymeter"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

var send = map[string]time.Time{}
var recieve = map[string]time.Time{}

var eventHubNamespaceConnectString = os.Getenv("EVENTHUB_NAMESPACE_CS")
var eventHubName = os.Getenv("EVENTHUB_NAME")
var storageConnectString = os.Getenv("STORAGE_CS")
var containerName = os.Getenv("CONTAINER_NAME")

func main() {
	ctx := context.Background()
	go produce(ctx, 0)

	consume(ctx)
}

func produce(ctx context.Context, off int) {
	producerClient, err := azeventhubs.NewProducerClientFromConnectionString(eventHubNamespaceConnectString, eventHubName, nil)

	if err != nil {
		panic(err)
	}

	defer producerClient.Close(context.TODO())
	time.Sleep(60 * time.Second)
	for {
		for i := 0; i < 10000; i++ {
			events := createEventsForSample(i + off)

			newBatchOptions := &azeventhubs.EventDataBatchOptions{}

			batch, err := producerClient.NewEventDataBatch(context.TODO(), newBatchOptions)

			if err != nil {
				panic(err)
			}

			for j := 0; j < len(events); j++ {
				err = batch.AddEventData(events[j], nil)
				if err != nil {
					panic(err)
				}
			}

			send[strconv.Itoa(i+off)] = time.Now()
			err = producerClient.SendEventDataBatch(context.TODO(), batch, nil)
			if err != nil {
				fmt.Printf(string(i)+"Error sending batch: ", err)
			}
		}

		fmt.Println("Sent: ", len(send), " Recieved: ", len(recieve))
		t := tachymeter.New(&tachymeter.Config{Size: 10000})

		for k, v := range recieve {
			diff := v.Sub(send[k])
			t.AddTime(diff)
		}
		fmt.Println(t.Calc())

		for k := range send {
			delete(send, k)
		}
		for k := range recieve {
			delete(recieve, k)
		}
	}
}

func createEventsForSample(i int) []*azeventhubs.EventData {
	return []*azeventhubs.EventData{
		{
			Body: []byte(strconv.Itoa(i)),
		},
	}
}

func consume(ctx context.Context) {
	checkClient, err := container.NewClientFromConnectionString(storageConnectString, containerName, nil)

	if err != nil {
		panic(err)
	}

	checkpointStore, err := checkpoints.NewBlobStore(checkClient, nil)

	if err != nil {
		panic(err)
	}

	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(eventHubNamespaceConnectString, eventHubName, azeventhubs.DefaultConsumerGroup, nil)

	if err != nil {
		panic(err)
	}

	defer consumerClient.Close(context.TODO())

	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)

	if err != nil {
		panic(err)
	}

	dispatchPartitionClients := func() {
		for {
			partitionClient := processor.NextPartitionClient(context.TODO())

			if partitionClient == nil {
				break
			}

			go func() {
				if err := processEvents(partitionClient); err != nil {
					panic(err)
				}
			}()
		}
	}

	go dispatchPartitionClients()

	processorCtx, processorCancel := context.WithCancel(context.TODO())
	defer processorCancel()

	if err := processor.Run(processorCtx); err != nil {
		panic(err)
	}
}

func processEvents(partitionClient *azeventhubs.ProcessorPartitionClient) error {
	defer closePartitionResources(partitionClient)
	for {
		receiveCtx, receiveCtxCancel := context.WithTimeout(context.TODO(), time.Minute)
		events, err := partitionClient.ReceiveEvents(receiveCtx, 1, nil)
		receiveCtxCancel()
		ts := time.Now()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		//fmt.Printf("Processing %d event(s)\n", len(events))

		for _, event := range events {
			recieve[string(event.Body)] = ts

			//fmt.Printf("Event received with body %v\n", string(event.Body))
		}

		//if len(events) != 0 {
		//	if err := partitionClient.UpdateCheckpoint(context.TODO(), events[len(events)-1]); err != nil {
		//		return err
		//	}
		//}

	}
}

func closePartitionResources(partitionClient *azeventhubs.ProcessorPartitionClient) {
	defer partitionClient.Close(context.TODO())
}
