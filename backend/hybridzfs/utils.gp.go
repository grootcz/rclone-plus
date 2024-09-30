package hybridzfs

import "regexp"

func IsPartialFile(name string) bool {
	patten := "^.*\\.[a-f0-9]{8}\\.partial$"
	isMatch, err := regexp.MatchString(patten, name)
	if err != nil {
		return false
	}

	return isMatch
}
