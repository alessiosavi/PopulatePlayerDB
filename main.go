package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/alessiosavi/OgameStats/datastructure/players"
	"github.com/alessiosavi/OgameStats/utils"
)

func main() {
	// Loading all players from http api
	p, err := players.LoadPlayers(inputRequest.Uni)
	check(err)

}

// Retrieve every stats for all the players that play the universe
func downloadPlayerData(player players.Players) {
	var url = "https://s166-it.ogame.gameforge.com/api/playerData.xml?id="

	for _, p := range player.Players {
		fname := "/home/alessiosavi/WORKSPACE/Go/GoStatOgame/data/playerdata/" + p.ID + ".xml"
		if resp, err := http.Get(url + p.ID); err != nil {
			fmt.Printf("Error with id [%s] | Err: %s", p.ID, err.Error())
		} else {
			resp, _ := utils.ReadBody(resp)
			ioutil.WriteFile(fname, resp, 0644)
		}
	}

}
