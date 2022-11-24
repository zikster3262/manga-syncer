package api

import "github.com/gin-gonic/gin"

type RegCtx struct {
	*gin.Context
}

func New(c *gin.Context) *RegCtx {
	return &RegCtx{
		Context: c,
	}
}
