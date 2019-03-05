package main

import (
	"context"
	"fmt"
)

func main() {
	SetVerboseLogging(false)

	result, err := squareNumbersPipeline(100)
	if err != nil {
		fmt.Printf("An error occurred during processing: %s", err.Error())
		return
	}

	for i := range result {
		fmt.Printf("%d -> %d\n", result[i].Input, result[i].Output)
	}
}

func squareNumbersPipeline(max int64) ([]Unit, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var errcList []<-chan error

	//Source Stage
	srcc, errc, err := NumbersSource(ctx, "source", max)
	if err != nil {
		return nil, err
	}
	errcList = append(errcList, errc)

	//Transformer Stage
	sq1c, errc, err := NumbersSquare(ctx, "squareNumbers1", srcc)
	if err != nil {
		return nil, err
	}
	errcList = append(errcList, errc)

	//Transformer Stage
	sq2c, errc, err := NumbersSquare(ctx, "squareNumbers2", sq1c)
	if err != nil {
		return nil, err
	}
	errcList = append(errcList, errc)

	//Sink Stage
	resc, errc, err := NumbersSink(ctx, "sink", sq2c)
	if err != nil {
		return nil, err
	}
	errcList = append(errcList, errc)

	//Wait for the pipeline to finish
	mergedErrc := MergeErrors(errcList...)
	for err := range mergedErrc {
		if err != nil {
			return nil, err
		}
	}

	//Pull the result value out
	result := <-resc

	return result, nil
}
