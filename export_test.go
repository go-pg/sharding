package sharding

import "math/rand"

func SetRandSeed(r *rand.Rand) {
	randSeed = r
}
