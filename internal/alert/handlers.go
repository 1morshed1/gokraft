package alert

// handlers.go exposes rule CRUD and current alert state over HTTP so the
// dashboard can manage alerts. Kept intentionally small; it is a thin layer over
// RuleStore and Evaluator.

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// API bundles what the alert endpoints need.
type API struct {
	Rules *RuleStore
	Eval  *Evaluator
}

// Register wires alert routes onto the group.
func (a *API) Register(r *gin.RouterGroup) {
	r.GET("/alerts/rules", a.listRules)
	r.POST("/alerts/rules", a.upsertRule)
	r.DELETE("/alerts/rules/:id", a.deleteRule)
}

// ruleWithState decorates a rule with its live evaluator state for the UI.
type ruleWithState struct {
	Rule
	State State `json:"state"`
}

func (a *API) listRules(c *gin.Context) {
	rules := a.Rules.List()
	out := make([]ruleWithState, 0, len(rules))
	for _, r := range rules {
		st := StateOK
		if a.Eval != nil {
			st = a.Eval.StateOf(r.ID)
		}
		out = append(out, ruleWithState{Rule: r, State: st})
	}
	c.JSON(http.StatusOK, gin.H{"rules": out})
}

func (a *API) upsertRule(c *gin.Context) {
	var r Rule
	if err := c.ShouldBindJSON(&r); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	stored, err := a.Rules.Upsert(r)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, stored)
}

func (a *API) deleteRule(c *gin.Context) {
	if a.Rules.Delete(c.Param("id")) {
		c.Status(http.StatusNoContent)
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "rule not found"})
}
