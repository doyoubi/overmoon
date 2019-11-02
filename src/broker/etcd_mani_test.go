package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateClusterName(t *testing.T) {
	assert := assert.New(t)

	assert.Nil(validateClusterName("abc-d"))
	assert.NotNil(validateClusterName(""))
	assert.NotNil(validateClusterName("a b"))
}
