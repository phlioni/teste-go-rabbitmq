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
	MarketID int      `json:"market_id"`
	Coords   Coords   `json:"coords"`
	ShopList ShopList `json:"shop_list"`
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
	ShopList     []int   `json:"shop_list"`
}

// Estrutura para o segundo JSON completo
type ReferenceData struct {
	Data struct {
		Market MarketReference `json:"market"`
	} `json:"data"`
}

type ShopList struct {
	BuyDate string `json:"buy_date"`
	List    []List `json:"list"`
}

type List struct {
	ProductID    int     `json:"product_id"`
	Product      string  `json:"product"`
	ProductValue float64 `json:"product_value"`
}

type MarketList struct {
	ListMarket []ListBuy
}

type ListBuy struct {
	MarketID int
	Location Coords
	List     []List
	SumList  float64
}

func main() {
	// Conectar e consumir mensagens simula o MongoDB
	marketsJSON := `{
   "markets":[
      {
         "market_id":2,
         "coords":{
            "coorx":-23.953367,
            "coory":-46.347802
         },
         "shop_list":{
            "buy_date":"2024-11-10",
            "list":[
               {
                  "product_id":1312,
                  "product":"Sabão em pó",
                  "product_value":12.30
               },
               {
                  "product_id":1354,
                  "product":"Amaciante",
                  "product_value":15.50
               },
               {
                  "product_id":6423,
                  "product":"Desinfetante",
                  "product_value":8.90
               },
               {
                  "product_id":2134,
                  "product":"Shampoo",
                  "product_value":18.75
               },
               {
                  "product_id":42564,
                  "product":"Condicionador",
                  "product_value":19.20
               },
               {
                  "product_id":4321,
                  "product":"Detergente",
                  "product_value":3.50
               },
               {
                  "product_id":23654,
                  "product":"Creme dental",
                  "product_value":5.75
               },
               {
                  "product_id":47653,
                  "product":"Esponja de aço",
                  "product_value":2.80
               },
               {
                  "product_id":653,
                  "product":"Sabonete",
                  "product_value":2.20
               },
               {
                  "product_id":1533,
                  "product":"Sabão líquido",
                  "product_value":14.00
               },
               {
                  "product_id":50001,
                  "product":"Café",
                  "product_value":6.50
               },
               {
                  "product_id":50002,
                  "product":"Açúcar",
                  "product_value":3.10
               },
               {
                  "product_id":50003,
                  "product":"Arroz",
                  "product_value":4.30
               },
               {
                  "product_id":50004,
                  "product":"Feijão",
                  "product_value":5.40
               },
               {
                  "product_id":50005,
                  "product":"Farinha de trigo",
                  "product_value":3.80
               }
            ]
         }
      },
      {
         "market_id":3,
         "coords":{
            "coorx":-23.955898,
            "coory":-46.348105
         },
         "shop_list":{
            "buy_date":"2024-11-10",
            "list":[
               {
                  "product_id":1235,
                  "product":"Água sanitária",
                  "product_value":4.99
               },
               {
                  "product_id":1312,
                  "product":"Sabão em pó",
                  "product_value":12.30
               },
               {
                  "product_id":2134,
                  "product":"Shampoo",
                  "product_value":17.50
               },
               {
                  "product_id":23654,
                  "product":"Creme dental",
                  "product_value":5.99
               },
               {
                  "product_id":1354,
                  "product":"Amaciante",
                  "product_value":16.30
               },
               {
                  "product_id":42564,
                  "product":"Condicionador",
                  "product_value":18.90
               },
               {
                  "product_id":4321,
                  "product":"Detergente",
                  "product_value":3.20
               },
               {
                  "product_id":6423,
                  "product":"Desinfetante",
                  "product_value":9.20
               },
               {
                  "product_id":1533,
                  "product":"Sabão líquido",
                  "product_value":13.50
               },
               {
                  "product_id":47653,
                  "product":"Esponja de aço",
                  "product_value":3.10
               },
               {
                  "product_id":50006,
                  "product":"Macarrão",
                  "product_value":4.20
               },
               {
                  "product_id":50007,
                  "product":"Óleo de cozinha",
                  "product_value":6.90
               },
               {
                  "product_id":50008,
                  "product":"Vinagre",
                  "product_value":2.70
               },
               {
                  "product_id":50009,
                  "product":"Sal",
                  "product_value":1.80
               },
               {
                  "product_id":50010,
                  "product":"Margarina",
                  "product_value":3.50
               }
            ]
         }
      },
      {
         "market_id":4,
         "coords":{
            "coorx":-23.968385,
            "coory":-46.402118
         },
         "shop_list":{
            "buy_date":"2024-11-10",
            "list":[
               {
                  "product_id":1312,
                  "product":"Sabão em pó",
                  "product_value":12.00
               },
               {
                  "product_id":1354,
                  "product":"Amaciante",
                  "product_value":16.80
               },
               {
                  "product_id":2134,
                  "product":"Shampoo",
                  "product_value":19.99
               },
               {
                  "product_id":4321,
                  "product":"Detergente",
                  "product_value":3.99
               },
               {
                  "product_id":42564,
                  "product":"Condicionador",
                  "product_value":20.10
               },
               {
                  "product_id":653,
                  "product":"Sabonete",
                  "product_value":2.00
               },
               {
                  "product_id":6423,
                  "product":"Desinfetante",
                  "product_value":9.50
               },
               {
                  "product_id":1533,
                  "product":"Sabão líquido",
                  "product_value":15.40
               },
               {
                  "product_id":47653,
                  "product":"Esponja de aço",
                  "product_value":2.60
               },
               {
                  "product_id":23654,
                  "product":"Creme dental",
                  "product_value":6.00
               },
               {
                  "product_id":50011,
                  "product":"Leite",
                  "product_value":4.60
               },
               {
                  "product_id":50012,
                  "product":"Iogurte",
                  "product_value":3.30
               },
               {
                  "product_id":50013,
                  "product":"Queijo",
                  "product_value":7.50
               },
               {
                  "product_id":50014,
                  "product":"Presunto",
                  "product_value":5.60
               },
               {
                  "product_id":50015,
                  "product":"Suco de laranja",
                  "product_value":4.40
               }
            ]
         }
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

	var listBuy = listCompare(result, referenceData)

	//TODO: Função para comparar listas
	jsonData, _ := json.Marshal(listBuy)
	fmt.Printf("Id: %s", jsonData)

}

func listCompare(markets []Market, referenceData ReferenceData) MarketList {
	// Mapa para armazenar os IDs de produtos do mercado de referência
	referenceProducts := make(map[int]bool)
	for _, refProductID := range referenceData.Data.Market.ShopList {
		referenceProducts[refProductID] = true
	}

	var listBuy []ListBuy
	for _, market := range markets {
		var matchedProducts []List
		for _, product := range market.ShopList.List {
			if _, found := referenceProducts[product.ProductID]; found {
				// Adiciona o produto encontrado à lista de produtos correspondentes
				matchedProducts = append(matchedProducts, product)
			}
		}

		// Se houver produtos correspondentes, adiciona à lista de compras
		if len(matchedProducts) > 0 {
			var sumList float64

			for _, product := range matchedProducts {
				sumList += product.ProductValue
			}

			listBuy = append(listBuy, ListBuy{
				MarketID: market.MarketID,
				Location: market.Coords,
				List:     matchedProducts,
				SumList:  sumList,
			})
		}
	}

	return MarketList{ListMarket: listBuy}
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
