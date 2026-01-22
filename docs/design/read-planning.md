# Read Planning

This document outlines the strategy for planning and executing reads from Tdms files, focusing on efficient operation.

## Hierarchy of Plans

The planning generates three levels of plans:

* File Plans
* Data Block Plans
* Record Plans

## File Plans

The goal of the file plan is to determine which blocks need to be read and in what way. We may have alternative read methods based on the type of read we want to complete.

The main case for that right now is if we want to skip some samples.

## Data Block Plans

A plan for a data block states which samples from which channels in the block need to be read.

## Record Plans

The record plan describes the size of the records in the block to allow for skipping records and unread data.
