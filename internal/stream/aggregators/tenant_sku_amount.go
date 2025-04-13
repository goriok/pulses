package aggregators

import (
	"fmt"
	"goriok/pulses/internal/models"
	"strings"
	"time"
)

func TenantSKUInfo(key, window string, total float64) (map[string]any, string, error) {
	pulse, err := parseTenantSKUKey(key)
	if err != nil {
		return nil, "", err
	}

	payload := map[string]any{
		"tenant_id":    pulse.TenantID,
		"product_sku":  pulse.ProductSKU,
		"use_unit":     pulse.UseUnity,
		"total_amount": total,
		"window":       window,
		"timestamp":    time.Now().Unix(),
	}

	topic := fmt.Sprintf("tenants.%s.aggregated.pulses.amount", pulse.TenantID)
	return payload, topic, nil
}

func TenantSKUKey(event any) string {
	p, ok := event.(*models.Pulse)
	if !ok {
		return "invalid"
	}
	return fmt.Sprintf("%s.%s.%s", p.TenantID, p.ProductSKU, p.UseUnity)
}

func TenantSKUAmount(event any) float64 {
	p, ok := event.(*models.Pulse)
	if !ok {
		return 0
	}
	return p.UsedAmmount
}

func parseTenantSKUKey(key string) (*models.Pulse, error) {
	parts := strings.Split(key, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid key format: %s", key)
	}

	return &models.Pulse{
		TenantID:   parts[0],
		ProductSKU: parts[1],
		UseUnity:   parts[2],
	}, nil
}
