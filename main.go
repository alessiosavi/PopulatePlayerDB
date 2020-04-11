package main

import (
	"github.com/alessiosavi/GoStatOgame/datastructure/players"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"log"
	"os"
	"sync"
)

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

	// Retrieve the player from API
	playerData := downloadPlayerData(166, p)

	// Initialize a new connection to DynamoDB
	svc := initDynamoDBConnection()

	// Converting the array player into dynamo put request
	var requests []map[string]*dynamodb.AttributeValue = make([]map[string]*dynamodb.AttributeValue, len(playerData))
	for i, pData := range playerData {
		if requests[i], err = dynamodbattribute.MarshalMap(pData); err != nil {
			panic(err)
		}
	}
	var i int = 0
	// Executing 25 request in batch
	for ; i < len(requests)-25; i += 25 {
		itemInput := dynamodb.BatchWriteItemInput{RequestItems:
		map[string][]*dynamodb.WriteRequest{tableName: {}}}
		var dynamoRequest []*dynamodb.WriteRequest
		// Loading the 25 request
		for j := i; j < i+25; j++ {
			log.Println("Managing ", j)
			var r dynamodb.WriteRequest
			r.PutRequest = &dynamodb.PutRequest{
				Item: requests[j],
			}
			dynamoRequest = append(dynamoRequest, &r)
		}
		itemInput.RequestItems[tableName] = append(itemInput.RequestItems[tableName], dynamoRequest...)
		result, err := svc.BatchWriteItem(&itemInput)
		if err != nil {
			log.Printf("Err: %+v\n", err)
		} else {
			log.Printf("Result: %+v\n", result)
		}
	}
	// Managing the other put request less than 25
	{
		itemInput := dynamodb.BatchWriteItemInput{RequestItems:
		map[string][]*dynamodb.WriteRequest{tableName: {}}}
		var dynamoRequest []*dynamodb.WriteRequest
		for ; i < len(requests); i++ {
			log.Println("Managing ", i)
			var r dynamodb.WriteRequest
			r.PutRequest = &dynamodb.PutRequest{
				Item: requests[i],
			}
			dynamoRequest = append(dynamoRequest, &r)
			itemInput.RequestItems[tableName] = append(itemInput.RequestItems[tableName], dynamoRequest...)
		}
		result, err := svc.BatchWriteItem(&itemInput)
		if err != nil {
			log.Printf("Err: %+v\n", err)
		} else {
			log.Printf("Result: %+v\n", result)
		}
	}

	for i = 0; i < len(playerData)-100; i += 100 {
		getInput := dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				tableName: {}}}
		for j := i; j < i+100; j++ {
			key := map[string]*dynamodb.AttributeValue{
				"ID":       &dynamodb.AttributeValue{S: aws.String(playerData[j].ID)},
				"Username": &dynamodb.AttributeValue{S: aws.String(playerData[j].Username)}}
			getInput.RequestItems[tableName].Keys = append(getInput.RequestItems[tableName].Keys, key)
		}
		result, err := svc.BatchGetItem(&getInput)
		if err != nil {
			panic(err)
		}
		var data []players.PlayerData
		err = dynamodbattribute.UnmarshalListOfMaps(result.Responses[tableName], &data)
		log.Printf("Data from dynamo\n%+v\n", data)
		log.Println("len data: ", len(data))
	}
	{
		getInput := dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				tableName: {}}}
		for ; i < len(requests); i++ {
			getInput := dynamodb.BatchGetItemInput{
				RequestItems: map[string]*dynamodb.KeysAndAttributes{
					tableName: {}}}

			key := map[string]*dynamodb.AttributeValue{
				"ID":       &dynamodb.AttributeValue{S: aws.String(playerData[i].ID)},
				"Username": &dynamodb.AttributeValue{S: aws.String(playerData[i].Username)}}
			getInput.RequestItems[tableName].Keys = append(getInput.RequestItems[tableName].Keys, key)
		}
		result, err := svc.BatchGetItem(&getInput)
		if err != nil {
			panic(err)
		}
		var data []players.PlayerData
		err = dynamodbattribute.UnmarshalListOfMaps(result.Responses[tableName], &data)
		log.Printf("Data from dynamo\n%+v\n", data)
		log.Println("len data: ", len(data))
	}

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
// TODO Add threading
func downloadPlayerData(uni int, player players.Players) []players.PlayerData {
	var playersData []players.PlayerData = make([]players.PlayerData, len(player.Players))
	var pData players.PlayerData
	var err error
	var wg sync.WaitGroup
	// Run 5 thread concurrently
	semaphore := make(chan struct{}, 10)

	for i, p := range player.Players {
		wg.Add(1)
		go func(i int, p players.Player) {
			semaphore <- struct{}{}
			if pData, err = players.RetrievePlayerDataByID(uni, p.ID); err != nil {
				log.Printf("Unable to retrieve data for user %s in uni %d", p.ID, uni)
			} else {
				pData.ID = p.ID
				pData.Username = p.Name
				playersData[i] = pData
			}
			func() { <-semaphore }()
			wg.Done()
		}(i, p)
	}
	wg.Wait()

	return playersData
}
