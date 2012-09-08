package aws

import (
	"fmt"
	"github.com/bmizerany/assert"
	"testing"
)

var testDomain string = "aws-golang-test-domain"

var boxUsage float64 = 0

func opsPerDollar(usage float64) int64 {
	return int64(1.0 / usage / 0.14)
}

func listDomains(t *testing.T) *SDBListDomainsResponse {
	list, err := SDBListDomains(10, "")
	if err != nil {
		t.Fatal(err)
	}
	boxUsage += list.BoxUsage
	return list
}

func deleteDomain(t *testing.T) {
	resp, err := SDBDeleteDomain(testDomain)
	if err != nil {
		t.Fatal(err)
	}
	boxUsage += resp.BoxUsage
}

func createDomain(t *testing.T) {
	resp, err := SDBCreateDomain(testDomain)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEqual(t, "", resp.RequestId)
	boxUsage += resp.BoxUsage
}

func domainMeta(t *testing.T) *SDBDomainMetaResponse {
	meta, err := SDBDomainMetadata(testDomain)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 0, meta.ItemCount)
	assert.Equal(t, true, meta.Timestamp > 0)
	boxUsage += meta.BoxUsage
	return meta
}

func putAttr(t *testing.T, item string, attribs []SDBAttribute) {
	resp, err := SDBPutAttributes(testDomain, item, attribs)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("putAttr boxUsage:", resp.BoxUsage, "ops/$", opsPerDollar(resp.BoxUsage))
	boxUsage += resp.BoxUsage
}

func getAttrs(t *testing.T, item string, attribs []string, consistent bool) *SDBGetAttributeResponse {
	resp, err := SDBGetAttributes(testDomain, item, attribs, consistent)
	if err != nil {
		t.Fatal(err)
	}
	boxUsage += resp.BoxUsage
	fmt.Println("getAttr boxUsage:", resp.BoxUsage, "ops/$", opsPerDollar(resp.BoxUsage))
	return resp
}

func deleteAttrs(t *testing.T, item string, attribs []SDBAttribute) {
	resp, err := SDBDeleteAttributes(testDomain, item, attribs)
	if err != nil {
		t.Fatal(err)
	}
	boxUsage += resp.BoxUsage
}

func batchPut(t *testing.T, items []SDBItem) {
	for _, item := range items {
		item.Attribs = []SDBAttribute{}
	}
	resp, err := SDBBatchPutAttributes(testDomain, items)
	if err != nil {
		t.Fatal(err)
	}
	boxUsage += resp.BoxUsage
}

func batchDelete(t *testing.T, items []SDBItem) {
	for _, item := range items {
		item.Attribs = []SDBAttribute{}
	}
	resp, err := SDBBatchDeleteAttributes(testDomain, items)
	if err != nil {
		t.Fatal(err)
	}
	boxUsage += resp.BoxUsage
}

func selectQuery(t *testing.T, query, nextToken string, consistent bool) *SDBSelectResponse {
	fmt.Println("selectQuery:", query)
	resp, err := SDBSelect(query, nextToken, consistent)
	if err != nil {
		t.Fatal(err)
	}
	boxUsage += resp.BoxUsage
	return resp
}

func TestSDBAll(t *testing.T) {
	boxUsage = 0

	list := listDomains(t)
	for _, d := range list.Domains {
		if d == testDomain {
			deleteDomain(t)
		}
	}

	createDomain(t)
	domainMeta(t)

	items := []SDBItem{
		{
			Name: "seattle",
			Attribs: []SDBAttribute{
				SDBAttribute{Name: "temp", Value: "58"},
				SDBAttribute{Name: "baseball", Value: "Mariners"},
				SDBAttribute{Name: "football", Value: "Seahawks"},
			}},
		{
			Name: "sf",
			Attribs: []SDBAttribute{
				SDBAttribute{Name: "temp", Value: "65 degrees"},
				SDBAttribute{Name: "baseball", Value: "Giants"},
				SDBAttribute{Name: "football", Value: "49ers"},
			}},
	}

	for _, item := range items {
		putAttr(t, item.Name, item.Attribs)
		attrResp := getAttrs(t, item.Name, []string{}, true)
		m := map[string]string{}
		for _, attrVal := range attrResp.Attribs {
			m[attrVal.Name] = attrVal.Value
		}
		for _, attr := range item.Attribs {
			assert.Equal(t, attr.Value, m[attr.Name])
		}
	}

	for _, item := range items {
		deleteAttrs(t, item.Name, []SDBAttribute{})
		attrResp := getAttrs(t, item.Name, []string{}, true)
		assert.Equal(t, 0, len(attrResp.Attribs))
	}

	// Batch put/get
	batchPut(t, items)
	selectResp := selectQuery(t, "select * from `"+testDomain+"`", "", true)
	assert.Equal(t, len(items), len(selectResp.Items))
	for x, item := range items {
		assert.Equal(t, item.Name, selectResp.Items[x].Name)
		assert.Equal(t, len(item.Attribs), len(selectResp.Items[x].Attribs))
	}

	batchDelete(t, items)
	selectResp = selectQuery(t, "select * from `"+testDomain+"`", "", true)
	assert.Equal(t, 0, len(selectResp.Items))

	deleteDomain(t)
	list = listDomains(t)
	for _, d := range list.Domains {
		if d == testDomain {
			t.Fatal("Test domain returned from ListDomains after DeleteDomain called")
		}
	}

	fmt.Println("TestSDBAll boxUsage:", boxUsage)
}
