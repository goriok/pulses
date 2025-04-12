package models

type Pulse struct {
	TenantID    string  `json:"tenant_id"`
	ProductSKU  string  `json:"product_sku"`
	UsedAmmount float64 `json:"used_ammount"`
	UseUnity    string  `json:"use_unity"`
}
