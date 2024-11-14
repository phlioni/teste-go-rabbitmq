package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"

	"github.com/streadway/amqp"
)

type Coords struct {
	CoorX float64 `json:"coorx"`
	CoorY float64 `json:"coory"`
}

// Estrutura para representar os mercados no primeiro JSON
type Market struct {
	MarketID int    `json:"market_id"`
	Coords   Coords `json:"coords"`
}

// Estrutura para representar a lista de mercados do primeiro JSON
type MarketsData struct {
	Markets []Market `json:"markets"`
}

// Estrutura para representar o segundo JSON com o mercado de referência e o raio de busca
type MarketReference struct {
	MarketID     int     `json:"market_id"`
	MarketRadius float64 `json:"market_ratio_km"`
	Location     Coords  `json:"location"`
}

// Estrutura para o segundo JSON completo
type ReferenceData struct {
	Data struct {
		Market MarketReference `json:"market"`
	} `json:"data"`
}

func main() {
	// Conectar e consumir mensagens
	marketsJSON := `{
		"markets": [
			{
				"market_id": 2,
				"coords": { "coorx": -23.953367, "coory": -46.347802 }
			},
			{
				"market_id": 3,
				"coords": { "coorx": -23.955898, "coory": -46.348105 }
			},
			{
				"market_id": 4,
				"coords": { "coorx": -23.968385, "coory": -46.402118 }
			},
			{
				"market_id": 5,
				"coords": { "coorx": -24.017151, "coory": -46.438122 }
			},
			{
				"market_id": 6,
				"coords": { "coorx": -23.957539, "coory": -46.345139 }
			}
		]
	}`

	err := StartConsuming(ProcessMessage, []byte(marketsJSON))
	if err != nil {
		log.Fatalf("Erro ao iniciar o consumidor: %v", err)
	}
}

func StartConsuming(process func(test []byte, marketjson []byte), marketjson []byte) error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("lista_teste", true, false, false, false, nil)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			process(msg.Body, marketjson)
		}
	}()

	select {}
}

func ProcessMessage(message []byte, marketjson []byte) {

	var marketsData MarketsData
	var referenceData ReferenceData

	if err := json.Unmarshal([]byte(marketjson), &marketsData); err != nil {
		fmt.Println("Erro ao decodificar JSON para ReferenceData:", err)
	}

	if err := json.Unmarshal([]byte(message), &referenceData); err != nil {
		fmt.Println("Erro ao decodificar JSON para MarketsData:", err)
	}

	result := filterMarketsWithinRadius(marketsData, referenceData)

	jsonData, _ := json.Marshal(result)

	fmt.Printf("Id: %s", jsonData)

}

func haversine(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371 // Raio da Terra em quilômetros
	lat1Rad := lat1 * math.Pi / 180
	lon1Rad := lon1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	lon2Rad := lon2 * math.Pi / 180

	dlat := lat2Rad - lat1Rad
	dlon := lon2Rad - lon1Rad

	a := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dlon/2)*math.Sin(dlon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c // Distância em km
}

func filterMarketsWithinRadius(markets MarketsData, reference ReferenceData) []Market {
	var result []Market

	for _, market := range markets.Markets {
		distance := haversine(reference.Data.Market.Location.CoorX, reference.Data.Market.Location.CoorY, market.Coords.CoorX, market.Coords.CoorY)
		if distance <= reference.Data.Market.MarketRadius {
			result = append(result, market)
		}
	}

	return result
}
