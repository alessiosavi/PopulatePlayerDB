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

// Convert the list of the players present in the universe in a map
func convertListPlayerToMap(p []players.Player) map[string]players.Player {
	var pMap map[string]players.Player = make(map[string]players.Player, len(p))
	for i := range p {
		pMap[p[i].ID] = p[i]
	}
	return pMap
}

func convertListPlayerDataToMap(p []players.PlayerData) *sync.Map {
	var pMap sync.Map
	for i := range p {
		pMap.Store(p[i].ID, p[i])
	}
	return &pMap
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	// Try to search the table_name from the lambda environment, or use fallback name "PlayerData"
	tableName := os.Getenv("table_name")
	if tableName == "" {
		log.Printf("WARNING! `table_name` not found in env!")
		tableName = "PlayerData"
	}

	var uni int = 166
	log.Printf("Loading players from uni %d\n", uni)
	// Retrieving all players from the http api
	p, err := players.LoadPlayers(uni)
	check(err)
	log.Printf("Loaded %d players from uni %d\n", len(p.Players), uni)

	log.Println("Converting list to map ...")
	// Convert them into a map for faster search
	pMap := convertListPlayerToMap(p.Players)
	log.Println("List converted into Map!")

	log.Println("Initializing a new connection to DynamoDB ...")
	// Initialize a new connection to DynamoDB
	svc := initDynamoDBConnection()
	log.Println("Connection to DynamoDB initialized!")

	log.Println("Retrieving players from dynamo ...")
	// Retrieve the users that are already present into DynamoDB, these ones have to be updated
	playersAlreadyPresentInDynamo, _ := retrieveDataFromDynamo(p.Players, tableName, svc)
	log.Printf("Retrieved %d players from dynamo!", len(playersAlreadyPresentInDynamo))

	// And convert them into a map for faster search
	log.Println("Converting list to map ...")
	pDataAlreadyPresent := convertListPlayerDataToMap(playersAlreadyPresentInDynamo)
	log.Println("List converted to map ...")
	playersAlreadyPresentInDynamo = nil

	// These users are the one that are not present into DynamoDB, so they have to be inserted
	var pDataToInsert sync.Map

	log.Println("Filtering user that have to be updated ...")
	// Iterating the map related to all the users
	for keyP := range pMap {
		// Check if the player related to the keyP is already present in DynamoDB
		if _, ok := pDataAlreadyPresent.Load(keyP); !ok {
			// If not present, than it have to be inserted into dynamo
			pDataToInsert.Store(pMap[keyP].ID, pMap[keyP])
		}
	}
	log.Printf("Need to update %d users and insert %d users\n", countMapEntries(pDataAlreadyPresent), countMapEntries(&pDataToInsert))
	// Now we have two sync.Map, thread safe, that contains:
	// pDataAlreadyPresent -> contains the user that have to be updated
	// pDataToInsert -> contains the user that have to be inserted

	// Download ALL the player data from the API
	playerData := downloadPlayerData(166, p)

	_, _ = insertDataIntoDynamo(playerData, tableName, svc)

}

func countMapEntries(pDataAlreadyPresent *sync.Map) int {
	var a int
	pDataAlreadyPresent.Range(func(_, _ interface{}) bool {
		a++
		return true
	})
	return a
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
			if err != nil {
				log.Printf("Error during insert %s\n", err.Error())
			}
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
			if err != nil {
				log.Printf("Error during insert %s\n", err.Error())
			}
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

func retrieveDataFromDynamo(p []players.Player, tableName string, svc *dynamodb.DynamoDB) ([]players.PlayerData, error) {
	// Retrieve the Item from dynamo
	var i = 0
	var data []players.PlayerData
	var result *dynamodb.BatchGetItemOutput
	var err error
	for ; i < len(p)-batchGetItemSize; i += batchGetItemSize {
		getInput := dynamodb.BatchGetItemInput{RequestItems: map[string]*dynamodb.KeysAndAttributes{tableName: {}}}
		for j := i; j < i+batchGetItemSize; j++ {
			key := map[string]*dynamodb.AttributeValue{
				"ID":       &dynamodb.AttributeValue{S: aws.String(p[j].ID)},
				"Username": &dynamodb.AttributeValue{S: aws.String(p[j].Name)}}
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
		for ; i < len(p); i++ {
			key := map[string]*dynamodb.AttributeValue{
				"ID":       &dynamodb.AttributeValue{S: aws.String(p[i].ID)},
				"Username": &dynamodb.AttributeValue{S: aws.String(p[i].Name)}}
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
	return data, nil
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
