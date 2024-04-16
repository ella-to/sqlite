package sqlite

import (
	"fmt"
	"math"
	"strings"
)

const (
	radToDeg float64 = 180 / math.Pi
	degToRad float64 = math.Pi / 180
)

func calculateDerivedPosition(latitude, longitude, distance, bearing float64) (lat float64, lon float64) {
	const earthRadius float64 = 6371000 // m

	latA := degToRad * latitude
	lonA := degToRad * longitude
	angularDistance := distance / earthRadius
	trueCourse := degToRad * bearing

	lat = math.Asin(math.Sin(latA)*math.Cos(angularDistance) + math.Cos(latA)*math.Sin(angularDistance)*math.Cos(trueCourse))

	dlon := math.Atan2(math.Sin(trueCourse)*math.Sin(angularDistance)*math.Cos(latA), math.Cos(angularDistance)-math.Sin(latA)*math.Sin(lat))

	lon = (math.Mod((lonA + dlon + math.Pi), (math.Pi * 2))) - math.Pi

	lat = radToDeg * lat
	lon = radToDeg * lon

	return lat, lon
}

func CreateCondSQL(latitude, longitude, distance float64) string {
	const mult float64 = 1.1

	var sb strings.Builder

	if latitude != 0 && longitude != 0 {
		latitude1, _ := calculateDerivedPosition(latitude, longitude, mult*distance, 0)
		_, longitude2 := calculateDerivedPosition(latitude, longitude, mult*distance, 90)
		latitude3, _ := calculateDerivedPosition(latitude, longitude, mult*distance, 180)
		_, longitude4 := calculateDerivedPosition(latitude, longitude, mult*distance, 270)

		sb.WriteString(fmt.Sprintf("(latitude > %.6f AND ", latitude3))
		sb.WriteString(fmt.Sprintf("latitude < %.6f AND ", latitude1))
		sb.WriteString(fmt.Sprintf("longitude < %.6f AND ", longitude2))
		sb.WriteString(fmt.Sprintf("longitude > %.6f)", longitude4))
	}

	if sb.Len() == 0 {
		sb.WriteString("( 1=1 )")
	}

	return sb.String()
}

func CreateDistanceSQL(latitude, longitude float64) string {
	var sb strings.Builder

	if latitude != 0 && longitude != 0 {
		sb.WriteString(
			fmt.Sprintf(
				"min((%f - latitude) * (%f - latitude) + (%f - longitude) * (%f - longitude)) AS distance",
				latitude,
				latitude,
				longitude,
				longitude,
			),
		)
	}

	if sb.Len() == 0 {
		sb.WriteString("( 1 = 1 )")
	}

	return sb.String()
}
