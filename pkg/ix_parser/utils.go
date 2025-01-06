package ixparser

import "slices"

func containsAddress(address string, others ...string) bool {
	return slices.Contains(others, address)
}
