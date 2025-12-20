package protocol

type RequestHeader struct {
	Size          int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
}

func (d *Decoder) ParseRequestHeader() (*RequestHeader, error) {
	size, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}

	apiKey, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}

	apiVersion, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}

	correlationID, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}

	return &RequestHeader{
		Size:          size,
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationID: correlationID,
	}, nil
}
