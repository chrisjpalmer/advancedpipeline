package main

import (
	"context"
	"sync"
)

type Unit struct {
	Input  int64
	Output int64
}

func NumbersSource(ctx context.Context, name string, size int64) (<-chan Unit, <-chan error, error) {
	logOpen(name)
	out := make(chan Unit)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		defer logClose(name)

		var i int64
		for i = 0; i < size; i++ {
			logInput(name, i)
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
	logOpen(name)
	out := make(chan Unit)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		defer logClose(name)
		for unit := range in {
			logInput(name, unit.Input)
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

func NumbersFanOut(ctx context.Context, name string, in <-chan Unit, fanFactor int) ([]<-chan Unit, <-chan error, error) {
	logOpen(name)
	outs := make([]chan Unit, fanFactor)
	retOuts := make([]<-chan Unit, fanFactor)
	errc := make(chan error, 1)

	//Init all output channels
	for i := 0; i < fanFactor; i++ {
		out := make(chan Unit)
		outs[i] = out
		retOuts[i] = out
	}

	//Start the go routine which fans the work to fanFactor goroutines
	go func() {
		defer func() {
			for i := range outs {
				close(outs[i])
			}
		}()
		defer close(errc)
		defer logClose(name)

		i := 0

		//Fan it out - we will do round robin fan out here. distribute work evenly among all channels.
		//You could however do a dynamic select and pass to the next available channel - https://play.golang.org/p/8zwvSk4kjx
		for unit := range in {
			channelIndex := i % fanFactor
			out := outs[channelIndex]

			select {
			case out <- unit:
			case <-ctx.Done():
				return
			}

			i++
		}
	}()

	return retOuts, errc, nil
}

func NumbersFanIn(ctx context.Context, name string, ins ...<-chan Unit) (<-chan Unit, <-chan error, error) {
	logOpen(name)
	out := make(chan Unit)
	errc := make(chan error, 1)
	var wg sync.WaitGroup

	//Add all the channels to the wait group
	wg.Add(len(ins))

	//Start receiver for each input channel
	for _, inputChannel := range ins { //Avoid common mistake, inputChannel reference will change so we need to copy it
		go func(in <-chan Unit) {
			defer wg.Done()
			for unit := range in {
				logInput(name, unit.Input)
				select {
				case out <- unit:
				case <-ctx.Done():
					return
				}
			}
		}(inputChannel)
	}

	//Create a cleanup function which waits until all the recievers have finished to close the 'out' and 'errc' channels
	go func() {
		wg.Wait()
		close(out)
		close(errc)
		logClose(name)
	}()

	return out, errc, nil
}

func NumbersSink(ctx context.Context, name string, in <-chan Unit) (<-chan []Unit, <-chan error, error) {
	logOpen(name)
	out := make(chan []Unit, 1)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		defer logClose(name)

		results := make([]Unit, 0, 0)

		//For each value, receive it and append it
		for unit := range in {
			logInput(name, unit.Input)
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
