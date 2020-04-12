package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/alessiosavi/GoStatOgame/datastructure/players"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// AWS limit for put/get batch operation
const batchWriteItemSize = 25
const batchGetItemSize = 100

type InputRequest struct {
	Uni string `json:"uni"`
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	// Loading all players from http api
	p, err := players.LoadPlayers(166)
	check(err)

	tableName := os.Getenv("table_name")
	if tableName == "" {
		tableName = "PlayerData"
	}

	// Download the player data from API
	playerData := downloadPlayerData(166, p)

	// Initialize a new connection to DynamoDB
	svc := initDynamoDBConnection()

	_, _ = insertDataIntoDynamo(playerData, tableName, svc)

	_, _ = retrieveDataFromDynamo(playerData, tableName, svc)

}

func insertDataIntoDynamo(playerData []players.PlayerData, tableName string, svc *dynamodb.DynamoDB) (map[string][]*dynamodb.WriteRequest, error) {

	var err error
	var result *dynamodb.BatchWriteItemOutput
	var unprocessed map[string][]*dynamodb.WriteRequest = make(map[string][]*dynamodb.WriteRequest)
	// Converting the array player into dynamo put request
	var requests []map[string]*dynamodb.AttributeValue = make([]map[string]*dynamodb.AttributeValue, len(playerData))
	for i, pData := range playerData {
		if requests[i], err = dynamodbattribute.MarshalMap(pData); err != nil {
			panic(err)
		}
	}
	var i int = 0
	// Executing 25 request in batch
	for ; i < len(requests)-batchWriteItemSize; i += batchWriteItemSize {
		itemInput := dynamodb.BatchWriteItemInput{RequestItems: map[string][]*dynamodb.WriteRequest{tableName: {}}}
		var dynamoRequest []*dynamodb.WriteRequest
		// Loading the 25 request
		for j := i; j < i+batchWriteItemSize; j++ {
			//log.Println("Managing ", j)
			var r dynamodb.WriteRequest
			r.PutRequest = &dynamodb.PutRequest{Item: requests[j]}
			dynamoRequest = append(dynamoRequest, &r)
		}
		itemInput.RequestItems[tableName] = dynamoRequest
		if result, err = svc.BatchWriteItem(&itemInput); err != nil || len(result.UnprocessedItems[tableName]) > 0 {
			if result != nil {
				unprocessed[tableName] = append(unprocessed[tableName], result.UnprocessedItems[tableName]...)
			}
			log.Printf("Error during insert %s\n", err.Error())
		}
		//log.Printf("Result: %+v\n", result)
	}
	// Managing the other put request, less than 25
	{
		itemInput := dynamodb.BatchWriteItemInput{RequestItems: map[string][]*dynamodb.WriteRequest{tableName: {}}}
		var dynamoRequest []*dynamodb.WriteRequest
		for ; i < len(requests); i++ {
			//log.Println("Managing ", i)
			var r dynamodb.WriteRequest
			r.PutRequest = &dynamodb.PutRequest{Item: requests[i]}
			dynamoRequest = append(dynamoRequest, &r)
		}
		itemInput.RequestItems[tableName] = dynamoRequest
		if result, err = svc.BatchWriteItem(&itemInput); err != nil || len(result.UnprocessedItems[tableName]) > 0 {
			if result != nil {
				unprocessed[tableName] = append(unprocessed[tableName], result.UnprocessedItems[tableName]...)
			}
			log.Printf("Error during insert %s\n", err.Error())
		}
		//log.Printf("Result: %+v\n", result)
	}
	if len(unprocessed) > 0 {
		err = errors.New(fmt.Sprintf("Unable to insert %d put request", len(unprocessed)))
	} else {
		err = nil
	}
	return unprocessed, err
}

func retrieveDataFromDynamo(playerData []players.PlayerData, tableName string, svc *dynamodb.DynamoDB) ([]players.PlayerData, error) {
	// Retrieve the Item from dynamo
	var i = 0
	var data []players.PlayerData
	var result *dynamodb.BatchGetItemOutput
	var err error
	for ; i < len(playerData)-batchGetItemSize; i += batchGetItemSize {
		getInput := dynamodb.BatchGetItemInput{RequestItems: map[string]*dynamodb.KeysAndAttributes{tableName: {}}}
		for j := i; j < i+batchGetItemSize; j++ {
			key := map[string]*dynamodb.AttributeValue{
				"ID":       &dynamodb.AttributeValue{S: aws.String(playerData[j].ID)},
				"Username": &dynamodb.AttributeValue{S: aws.String(playerData[j].Username)}}
			getInput.RequestItems[tableName].Keys = append(getInput.RequestItems[tableName].Keys, key)
		}
		if result, err = svc.BatchGetItem(&getInput); err != nil {
			return nil, err
		}
		var tmp []players.PlayerData
		if err = dynamodbattribute.UnmarshalListOfMaps(result.Responses[tableName], &tmp); err != nil {
			return nil, err
		}
		data = append(data, tmp...)
	}
	{
		getInput := dynamodb.BatchGetItemInput{RequestItems: map[string]*dynamodb.KeysAndAttributes{tableName: {}}}
		for ; i < len(playerData); i++ {
			key := map[string]*dynamodb.AttributeValue{
				"ID":       &dynamodb.AttributeValue{S: aws.String(playerData[i].ID)},
				"Username": &dynamodb.AttributeValue{S: aws.String(playerData[i].Username)}}
			getInput.RequestItems[tableName].Keys = append(getInput.RequestItems[tableName].Keys, key)
		}
		if result, err = svc.BatchGetItem(&getInput); err != nil {
			return nil, err
		}
		var tmp []players.PlayerData
		if err = dynamodbattribute.UnmarshalListOfMaps(result.Responses[tableName], &tmp); err != nil {
			return nil, err
		}
		data = append(data, tmp...)
	}
	return playerData, nil
}

func initDynamoDBConnection() *dynamodb.DynamoDB {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	// Create DynamoDB client
	svc := dynamodb.New(sess)
	return svc
}

// Retrieve every stats for all the players that play the universe
func downloadPlayerData(uni int, player players.Players) []players.PlayerData {
	var playersData []players.PlayerData
	var pData players.PlayerData
	var err error
	var wg sync.WaitGroup
	// Run 3 thread concurrently
	semaphore := make(chan struct{}, 3)

	for i, p := range player.Players {
		wg.Add(1)
		go func(i int, p players.Player) {
			semaphore <- struct{}{}
			if pData, err = players.RetrievePlayerDataByID(uni, p.ID); err != nil {
				log.Printf("Unable to retrieve data for user %s in uni %d", p.ID, uni)
			} else {
				pData.ID = p.ID
				pData.Username = p.Name
				playersData = append(playersData, pData)
			}
			func() { <-semaphore }()
			wg.Done()
		}(i, p)
	}
	wg.Wait()

	return playersData
}
