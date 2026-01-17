package db

import (
	"encoding/json"
	"fmt"
)

const (
	maxMapJSONBytes        = 2 << 20
	maxMapFeatures         = 2000
	maxMapPointsPerFeature = 200
)

// ValidateMapJSON valida mida, estructura bàsica i límits de punts.
func ValidateMapJSON(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("map json empty")
	}
	if len(data) > maxMapJSONBytes {
		return fmt.Errorf("map json too large")
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("invalid map json")
	}
	layers, _ := payload["layers"].(map[string]interface{})
	if layers == nil {
		return nil
	}
	features := 0
	checkPoints := func(item interface{}) int {
		obj, ok := item.(map[string]interface{})
		if !ok {
			return 0
		}
		points, ok := obj["points"].([]interface{})
		if !ok {
			return 0
		}
		return len(points)
	}
	countLayer := func(key string, checkPointsLimit bool) error {
		raw, ok := layers[key]
		if !ok {
			return nil
		}
		arr, ok := raw.([]interface{})
		if !ok {
			return fmt.Errorf("invalid layer %s", key)
		}
		for _, item := range arr {
			features++
			if features > maxMapFeatures {
				return fmt.Errorf("too many features")
			}
			if checkPointsLimit {
				if pts := checkPoints(item); pts > maxMapPointsPerFeature {
					return fmt.Errorf("too many points")
				}
			}
		}
		return nil
	}
	if err := countLayer("houses", true); err != nil {
		return err
	}
	if err := countLayer("streets", true); err != nil {
		return err
	}
	if err := countLayer("rivers", true); err != nil {
		return err
	}
	if err := countLayer("elements", false); err != nil {
		return err
	}
	if err := countLayer("bounds", true); err != nil {
		return err
	}
	if err := countLayer("toponyms", false); err != nil {
		return err
	}
	return nil
}
