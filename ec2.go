package aws

const (
	EC2Host    = "ec2.amazonaws.com"
	EC2Version = "2011-11-01"
)

func (c *Client) EC2Request() *Request {
	return c.NewRequest(EC2Host, EC2Version)
}

type DescribeInstancesResponse struct {
	Header
	Reservations []Reservation `xml:"reservationSet>item"`
}

type Reservation struct {
	ReservationId string
	Instances     []Instance `xml:"instancesSet>item"`
}

type Instance struct {
	InstanceId string
	StateName  string `xml:"instanceState>name"`
	DnsName    string
	IpAddress  string
}

func (c *Client) DescribeInstances() (*DescribeInstancesResponse, error) {
	r := c.EC2Request()
	r.Add("Action", "DescribeInstances")

	v := new(DescribeInstancesResponse)
	return v, Do(r, v)
}

func DescribeInstances() (*DescribeInstancesResponse, error) {
	c := NewClient()
	return c.DescribeInstances()
}
