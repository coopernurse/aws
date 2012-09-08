package aws

import (
	"fmt"
	"strconv"
)

const (
	SDBHost    = "sdb.amazonaws.com"
	SDBVersion = "2009-04-15"
)

func (c *Client) SDBRequest() *Request {
	return c.NewRequest(SDBHost, SDBVersion)
}

type SDBResponse struct {
	RequestId    string      `xml:"ResponseMetadata>RequestId"`
	BoxUsage     float64     `xml:"ResponseMetadata>BoxUsage"`
}

type SDBListDomainsResponse struct {
	SDBResponse
	Domains          []string   `xml:"ListDomainsResult>DomainName"`
	NextToken        string     `xml:"ListDomainsResult>NextToken"`
}

type SDBDomainMetaResponse struct {
	SDBResponse
	Timestamp                int64   `xml:"DomainMetadataResult>Timestamp"`
    ItemCount                int     `xml:"DomainMetadataResult>ItemCount"`
    AttrValueCount      int     `xml:"DomainMetadataResult>AttributeValueCount"`
	AttrNameCount       int     `xml:"DomainMetadataResult>AttributeNameCount"`
	ItemNameSizeBytes        int64   `xml:"DomainMetadataResult>ItemNameSizeBytes"`
	AttrValuesSizeBytes int64   `xml:"DomainMetadataResult>AttributeValuesSizeBytes"`
	AttrNamesSizeBytes  int64   `xml:"DomainMetadataResult>AttributeNamesSizeBytes"`
}

type SDBItem struct {
	Name       string
	Attribs    []SDBItemAttribute
}

type SDBItemAttribute struct {
	Name            string
	Value           string
	Replace         bool
}

type SDBAttribute struct {
	Name            string
	Value           string
	Replace         bool
	ExpectedName    string
    ExpectedValue   string
	ExpectedExists  bool
}

type AttributeValue struct {
    Name     string   `xml:"Name"`
    Value    string   `xml:"Value"`
}

type SDBGetAttributeResponse struct {
	SDBResponse
	Attribs       []AttributeValue `xml:"GetAttributesResult>Attribute"`
}

func (c *Client) SDBBatchPutAttributes(domain string, items []SDBItem) (*SDBResponse, error) {
	r := c.SDBRequest()
	r.Add("Action", "BatchPutAttributes")
	r.Add("DomainName", domain)

	for y, item := range items {
		r.Add(fmt.Sprintf("Item.%d.ItemName", y), item.Name)
		for x, a := range item.Attribs {
			r.Add(fmt.Sprintf("Item.%d.Attribute.%d.Name", y, x), a.Name)
			r.Add(fmt.Sprintf("Item.%d.Attribute.%d.Value", y, x), a.Value)
			if a.Replace {
				r.Add(fmt.Sprintf("Item.%d.Attribute.%d.Replace", y, x), "true")
			}
		}
	}

	resp := new(SDBResponse)
	return resp, Do(r, resp)
}

func SDBBatchPutAttributes(domain string, items []SDBItem) (*SDBResponse, error) {
	c := NewClient()
	return c.SDBBatchPutAttributes(domain, items)
}

func (c *Client) SDBPutAttributes(domain, item string, attribs []SDBAttribute) (*SDBResponse, error) {
	r := c.SDBRequest()
	r.Add("Action", "PutAttributes")
	r.Add("DomainName", domain)
	r.Add("ItemName", item)

	for x, a := range attribs {
		r.Add(fmt.Sprintf("Attribute.%d.Name", x), a.Name)
		r.Add(fmt.Sprintf("Attribute.%d.Value", x), a.Value)
		if a.Replace {
			r.Add(fmt.Sprintf("Attribute.%d.Replace", x), "true")
		}
		if a.ExpectedName != "" {
			r.Add(fmt.Sprintf("Expected.%d.Name", x), a.ExpectedName)
			if a.ExpectedExists {
				r.Add(fmt.Sprintf("Expected.%d.Exists", x), "true")
			}
			if a.ExpectedValue != "" {
				r.Add(fmt.Sprintf("Expected.%d.Value", x), a.ExpectedValue)
			}
		}
	}

	resp := new(SDBResponse)
	return resp, Do(r, resp)
}

func SDBPutAttributes(domain, item string, attribs []SDBAttribute) (*SDBResponse, error) {
	c := NewClient()
	return c.SDBPutAttributes(domain, item, attribs)
}

func (c *Client) SDBGetAttributes(domain, item string, attribs []string, consistent bool) (*SDBGetAttributeResponse, error) {
	r := c.SDBRequest()
	r.Add("Action", "GetAttributes")
	r.Add("DomainName", domain)
	r.Add("ItemName", item)
	for x, attr := range attribs {
		r.Add(fmt.Sprintf("AttributeName.%d", x), attr)
	}
	if consistent {
		r.Add("ConsistentRead", "true")
	}
	
	resp := new(SDBGetAttributeResponse)
	return resp, Do(r, resp)
}

func SDBGetAttributes(domain, item string, attribs []string, consistent bool) (*SDBGetAttributeResponse, error) {
	c := NewClient()
	return c.SDBGetAttributes(domain, item, attribs, consistent)
}

func (c *Client) SDBDeleteAttributes(domain, item string, attribs []SDBAttribute) (*SDBResponse, error) {
	r := c.SDBRequest()
	r.Add("Action", "DeleteAttributes")
	r.Add("DomainName", domain)
	r.Add("ItemName", item)

	for x, a := range attribs {
		r.Add(fmt.Sprintf("Attribute.%d.Name", x), a.Name)
		if a.Value != "" {
			r.Add(fmt.Sprintf("Attribute.%d.Value", x), a.Value)
		}

		if a.ExpectedName != "" {
			r.Add(fmt.Sprintf("Expected.%d.Name", x), a.ExpectedName)
			if a.ExpectedExists {
				r.Add(fmt.Sprintf("Expected.%d.Exists", x), "true")
			}
			if a.ExpectedValue != "" {
				r.Add(fmt.Sprintf("Expected.%d.Value", x), a.ExpectedValue)
			}
		}
	}

	resp := new(SDBResponse)
	return resp, Do(r, resp)
}

func SDBDeleteAttributes(domain, item string, attribs []SDBAttribute) (*SDBResponse, error) {
	c := NewClient()
	return c.SDBDeleteAttributes(domain, item, attribs)
}

func (c *Client) SDBListDomains(maxDomains int, nextToken string) (*SDBListDomainsResponse, error) {
	r := c.SDBRequest()
	r.Add("Action", "ListDomains")
	if maxDomains > 0 {
		r.Add("MaxNumberOfDomains", strconv.Itoa(maxDomains))
	}
	if nextToken != "" {
		r.Add("NextToken", nextToken)
	}

	resp := new(SDBListDomainsResponse)
	return resp, Do(r, resp)
}

func SDBListDomains(maxDomains int, nextToken string) (*SDBListDomainsResponse, error) {
	c := NewClient()
	return c.SDBListDomains(maxDomains, nextToken)
}

func (c *Client) SDBCreateDomain(domain string) (*SDBResponse, error) {
	r := c.SDBRequest()
	r.Add("Action", "CreateDomain")
	r.Add("DomainName", domain)

	resp := new(SDBResponse)
	return resp, Do(r, resp)
}

func SDBCreateDomain(domain string) (*SDBResponse, error) {
	c := NewClient()
	return c.SDBCreateDomain(domain)
}

func (c *Client) SDBDeleteDomain(domain string) (*SDBResponse, error) {
	r := c.SDBRequest()
	r.Add("Action", "DeleteDomain")
	r.Add("DomainName", domain)

	resp := new(SDBResponse)
	return resp, Do(r, resp)
}

func SDBDeleteDomain(domain string) (*SDBResponse, error) {
	c := NewClient()
	return c.SDBDeleteDomain(domain)
}

func (c *Client) SDBDomainMetadata(domain string) (*SDBDomainMetaResponse, error) {
	r := c.SDBRequest()
	r.Add("Action", "DomainMetadata")
	r.Add("DomainName", domain)

	resp := new(SDBDomainMetaResponse)
	return resp, Do(r, resp)
}

func SDBDomainMetadata(domain string) (*SDBDomainMetaResponse, error) {
	c := NewClient()
	return c.SDBDomainMetadata(domain)
}