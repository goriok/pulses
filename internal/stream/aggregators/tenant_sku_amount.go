package aggregators

import (
	"fmt"
	"goriok/pulses/internal/models"
)

// TenantSKUKey builds a key using TenantID and ProductSKU
func TenantSKUKey(event any) string {
	p, ok := event.(*models.Pulse)
	if !ok {
		return "invalid"
	}
	return fmt.Sprintf("%s|%s", p.TenantID, p.ProductSKU)
}

// TenantSKUAmount returns the UsedAmmount field from a Pulse
func TenantSKUAmount(event any) float64 {
	p, ok := event.(*models.Pulse)
	if !ok {
		return 0
	}
	return p.UsedAmmount
}
