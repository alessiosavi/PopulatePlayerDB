package main

import (
	"fmt"
	"github.com/alessiosavi/GoStatOgame/datastructure/players"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"os"
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
	// Loading all players from http api
	p, err := players.LoadPlayers(166)
	check(err)

	tableName := os.Getenv("table_name")
	if tableName == "" {
		tableName = "PlayerData"
	}

	playerData := downloadPlayerData(166, p)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	// Creating the array of request
	var requests []map[string]*dynamodb.AttributeValue = make([]map[string]*dynamodb.AttributeValue, len(playerData))
	for i, pData := range playerData {
		if requests[i], err = dynamodbattribute.MarshalMap(pData); err != nil {
			panic(err)
		}
	}
	var i int = 0
	//for ; i < len(requests)-25; i += 25 {
	for ; i < len(requests); i += 2 {
		itemInput := dynamodb.BatchWriteItemInput{RequestItems:
		map[string][]*dynamodb.WriteRequest{tableName: {}}}
		var dynamoRequest []*dynamodb.WriteRequest
		//for j := i; j < i+; j++ {
		for j := i; j < i+2; j++ {
			fmt.Println("Managing ", j)
			var r dynamodb.WriteRequest
			r.PutRequest = &dynamodb.PutRequest{
				Item: requests[j],
			}
			dynamoRequest = append(dynamoRequest, &r)
		}
		fmt.Println("Length dynamo: ", len(dynamoRequest))
		itemInput.RequestItems[tableName] = append(itemInput.RequestItems[tableName], dynamoRequest...)
		result, err := svc.BatchWriteItem(&itemInput)
		if err != nil {
			fmt.Printf("Err: %+v\n", err)
		}
		fmt.Printf("Result: %+v\n", result)
	}

	getInput := dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			tableName: {}}}
	for _, p := range playerData {
		key := map[string]*dynamodb.AttributeValue{
			"ID":       &dynamodb.AttributeValue{S: aws.String(p.ID)},
			"Username": &dynamodb.AttributeValue{S: aws.String(p.Username)}}
		getInput.RequestItems[tableName].Keys = append(getInput.RequestItems[tableName].Keys, key)
	}

	fmt.Printf("Query: %+v\n", getInput)
	result, err := svc.BatchGetItem(&getInput)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", result)

	var data []players.PlayerData

	err = dynamodbattribute.UnmarshalListOfMaps(result.Responses[tableName], &data)

	fmt.Printf("Data from dynamo\n%+v\n", data)
}

// Retrieve every stats for all the players that play the universe
func downloadPlayerData(uni int, player players.Players) []players.PlayerData {
	var playersData []players.PlayerData
	var pData players.PlayerData
	var err error

	for _, p := range player.Players[:2] {
		if pData, err = players.RetrievePlayerDataByID(uni, p.ID); err != nil {
			fmt.Printf("Unable to retrieve data for user %s in uni %d", p.ID, uni)
			continue
		}
		pData.ID = p.ID
		pData.Username = p.Name
		playersData = append(playersData, pData)
	}
	return playersData
}
