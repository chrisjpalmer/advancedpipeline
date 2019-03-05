package main

import (
	"context"
	"fmt"
	"sync"
)

type Unit struct {
	Input  int64
	Output int64
}

var verbose bool

func SetVerboseLogging(_verbose bool) {
	verbose = _verbose
}

func log(name string, input int64) {
	if verbose {
		fmt.Println(name+":", input)
	}
}

func NumbersSource(ctx context.Context, name string, size int64) (<-chan Unit, <-chan error, error) {
	out := make(chan Unit)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)

		var i int64
		for i = 0; i < size; i++ {
			log(name, i)
			newUnit := Unit{i, i}

			select {
			case out <- newUnit:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, errc, nil
}

// in <-chan TYPE is a channel producing values to be consumed
// <-chan values are leaving the channel
// out chan<- TYPE is a channel which you should push values into
// chan<- values are entering the channel

func NumbersSquare(ctx context.Context, name string, in <-chan Unit) (<-chan Unit, <-chan error, error) {
	out := make(chan Unit)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		for unit := range in {
			log(name, unit.Input)
			//Perform the transformation here:
			newOutput := unit.Output * unit.Output
			newUnit := Unit{unit.Input, newOutput}

			select {
			case out <- newUnit:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errc, nil
}

func NumbersSink(ctx context.Context, name string, in <-chan Unit) (<-chan []Unit, <-chan error, error) {
	out := make(chan []Unit, 1)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)

		results := make([]Unit, 0, 0)

		//For each value, receive it and append it
		for unit := range in {
			log(name, unit.Input)
			results = append(results, unit)
		}

		//In has been closed.
		//We wait to see who wins this race, will it be context OR the results channel being read?
		select {
		case out <- results:
		case <-ctx.Done():
			return
		}
	}()
	return out, errc, nil
}

// WaitForPipeline waits for results from all error channels.
// It returns early on the first error.
func WaitForPipeline(errs ...<-chan error) error {
	errc := MergeErrors(errs...)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	return nil
}

// MergeErrors - takes an array of error channels, waits on all of them and merges their outputs into a single error channel.
// This error channel will be closed when all the error channels in the input array are closed by their pipeline stages.
// Closing the resulting error channel is also a sign that the entire pipeline has finished executing.
func MergeErrors(cs ...<-chan error) <-chan error {
	var wg sync.WaitGroup

	// We must ensure that the output channel has the capacity to
	// hold as many errors
	// as there are error channels.
	// This will ensure that it never blocks, even
	// if WaitForPipeline returns early.

	out := make(chan error, len(cs))

	// Start an reciever goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls
	// wg.Done.
	wg.Add(len(cs))
	for _, errorChannel := range cs {
		go func(c <-chan error) {
			for n := range c {
				out <- n
			}
			wg.Done()
		}(errorChannel)
	}

	// Start a goroutine to close out once all the output goroutines
	// are done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
