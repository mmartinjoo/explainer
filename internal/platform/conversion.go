package platform

import "fmt"

func ConvertString(val interface{}) (string, error) {
	str, ok := val.([]byte)
	if !ok {
		return "", fmt.Errorf("platform.ConvertString failed: %v", val)
	}
	return string(str), nil
}
