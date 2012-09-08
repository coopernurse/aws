package aws

import (
	"github.com/bmizerany/assert"
	"testing"
)

var testDomain string = "aws-golang-test-domain"

func listDomains(t *testing.T) *SDBListDomainsResponse {
	list, err := SDBListDomains(10, "")
	if err != nil {
		t.Fatal(err)
	}
	return list
}

func deleteDomain(t *testing.T) {
	_, err := SDBDeleteDomain(testDomain)
	if err != nil {
		t.Fatal(err)
	}
}

func createDomain(t *testing.T) {
	resp, err := SDBCreateDomain(testDomain)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEqual(t, "", resp.RequestId)
}

func domainMeta(t *testing.T) *SDBDomainMetaResponse {
	meta, err := SDBDomainMetadata(testDomain)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 0, meta.ItemCount)
	assert.Equal(t, true, meta.Timestamp > 0)
	return meta
}

func putAttr(t *testing.T, item string, attribs []SDBAttribute) {
	_, err := SDBPutAttributes(testDomain, item, attribs)
	if err != nil {
		t.Fatal(err)
	}
}

func getAttrs(t *testing.T, item string, attribs []string, consistent bool) *SDBGetAttributeResponse {
	resp, err := SDBGetAttributes(testDomain, item, attribs, consistent)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func deleteAttrs(t *testing.T, item string, attribs []SDBAttribute) {
	_, err := SDBDeleteAttributes(testDomain, item, attribs)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSDBDomains(t *testing.T) {

	list := listDomains(t)
	for _, d := range list.Domains {
		if d == testDomain {
			deleteDomain(t)
		}
	}

	createDomain(t)
	domainMeta(t)

	items := map[string][]SDBAttribute{ 
		"seattle": []SDBAttribute{
			SDBAttribute{Name:"temp", Value: "58"},
			SDBAttribute{Name:"baseball", Value: "Mariners"},
			SDBAttribute{Name:"football", Value: "Seahawks"},
		},
		"sf": []SDBAttribute{
			SDBAttribute{Name:"temp", Value: "65"},
			SDBAttribute{Name:"baseball", Value: "Giants"},
			SDBAttribute{Name:"football", Value: "49ers"},
		},
	}

	for item, attrs := range items {
		putAttr(t, item, attrs)
		attrResp := getAttrs(t, item, []string{}, true)
		m := map[string]string{}
		for _, attrVal := range attrResp.Attribs {
			m[attrVal.Name] = attrVal.Value
		}
		for _, attr := range attrs {
			assert.Equal(t, attr.Value, m[attr.Name])
		}
	}

	for item, _ := range items {
		deleteAttrs(t, item, []SDBAttribute{})
		attrResp := getAttrs(t, item, []string{}, true)
		assert.Equal(t, 0, len(attrResp.Attribs))
	}

	deleteDomain(t)
	list = listDomains(t)
	for _, d := range list.Domains {
		if d == testDomain {
			t.Fatal("Test domain returned from ListDomains after DeleteDomain called")
		}
	}
}

