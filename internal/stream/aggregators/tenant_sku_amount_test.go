package aggregators

import (
	"goriok/pulses/internal/models"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- Test TenantSKUKey ---

func TestTenantSKUKey_ValidPulse(t *testing.T) {
	p := &models.Pulse{
		TenantID:   "tenant123",
		ProductSKU: "sku456",
		UseUnity:   "unit789",
	}

	key := TenantSKUKey(p)
	assert.Equal(t, "tenant123.sku456.unit789", key)
}

func TestTenantSKUKey_InvalidType(t *testing.T) {
	key := TenantSKUKey("not a pulse")
	assert.Equal(t, "invalid", key)
}

// --- Test TenantSKUAmount ---

func TestTenantSKUAmount_ValidPulse(t *testing.T) {
	p := &models.Pulse{
		UsedAmmount: 42.5,
	}

	amount := TenantSKUAmount(p)
	assert.Equal(t, 42.5, amount)
}

func TestTenantSKUAmount_InvalidType(t *testing.T) {
	amount := TenantSKUAmount("invalid type")
	assert.Equal(t, 0.0, amount)
}

// --- Test parseTenantSKUKey ---

func TestParseTenantSKUKey_Valid(t *testing.T) {
	key := "tenant1.sku2.unit3"
	p, err := parseTenantSKUKey(key)
	assert.NoError(t, err)
	assert.Equal(t, "tenant1", p.TenantID)
	assert.Equal(t, "sku2", p.ProductSKU)
	assert.Equal(t, "unit3", p.UseUnity)
}

func TestParseTenantSKUKey_InvalidFormat(t *testing.T) {
	key := "too.many.parts.here"
	_, err := parseTenantSKUKey(key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid key format")
}

// --- Test TenantSKUInfo ---

func TestTenantSKUInfo_Valid(t *testing.T) {
	key := "tenantX.skuY.unitZ"
	window := "5m"
	total := 12.34

	payload, topic, err := TenantSKUInfo(key, window, total)

	assert.NoError(t, err)
	assert.Equal(t, "tenantX", payload["tenant_id"])
	assert.Equal(t, "skuY", payload["product_sku"])
	assert.Equal(t, "unitZ", payload["use_unit"])
	assert.Equal(t, total, payload["total_amount"])
	assert.Equal(t, window, payload["window"])
	assert.IsType(t, int64(0), payload["timestamp"])
	assert.Equal(t, "tenants.tenantX.aggregated.pulses.amount", topic)
}

func TestTenantSKUInfo_InvalidKey(t *testing.T) {
	key := "invalid.key"
	_, _, err := TenantSKUInfo(key, "1h", 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid key format")
}
