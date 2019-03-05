package main

import (
	"context"
	"fmt"
)

func main() {
	result, err := squareNumbersPipeline1(100)
	if err != nil {
		fmt.Printf("An error occurred during processing: %s", err.Error())
		return
	}

	for i := range result {
		fmt.Printf("%d -> %d\n", result[i].Input, result[i].Output)
	}

	FlushLogs()

	result2, err := squareNumbersPipeline2(100)
	if err != nil {
		fmt.Printf("An error occurred during processing: %s", err.Error())
		return
	}

	for i := range result2 {
		fmt.Printf("%d -> %d\n", result2[i].Input, result2[i].Output)
	}

	FlushLogs()
}

func squareNumbersPipeline1(max int64) ([]Unit, error) {
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

func squareNumbersPipeline2(max int64) ([]Unit, error) {
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

	//Fan out the channel into 4 parallel channels which divide the work
	sq1cs, errc, err := NumbersFanOut(ctx, "numbersFanOut", sq1c, 4)
	if err != nil {
		return nil, err
	}
	errcList = append(errcList, errc)

	//Square the numbers again
	sq2cs := make([]<-chan Unit, 4)
	for i, sq1c := range sq1cs {
		//Transformer Stage
		sq2c, errc, err := NumbersSquare(ctx, fmt.Sprintf("%s:%d", "squareNumbers2", i), sq1c)
		if err != nil {
			return nil, err
		}
		errcList = append(errcList, errc)

		sq2cs[i] = sq2c
	}

	//Fan in the parallel work
	sq2c, errc, err := NumbersFanIn(ctx, "numbersFanIn", sq2cs...)
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
