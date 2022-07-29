# Lambda Architecture using AWS Services 
The project integrates Batch processing and Stream processing for a Extract-Transform-Load Pipeline using AWS Services called from Command Line. 

# Table of Contents
1. [Overview of the System Architecture](#overview)
2. [Installation and Running](#installation-and-running)
3. [Test Scripts](#test-scripts) 

# Overview
## Introduction to Lambda Architecture
Lambda Architecture was an ETL Pipeline developed to unify batch processing and stream processing to improve the availability of data. Developed as a paradigm with no fixed software, Lambda Architecture gives Data Engineers the freedom to set up the pipeline using services and a cloud provider that best meets their requirements. Although Lambda Architecture has been around for over a decade, it serves as a good starting point for anyone venturing into data engineering. A follow-up to lambda architecture is Kappa Architecture and Delta Architecture. 

## Current Architecture 
Lambda Architecture is developed in three main layers: Batch, Streaming and Serving. The Batch and Streaming layers work independently and can be scaled depending on the incoming workload. The serving layer is a common dashboard to run queries and generate views on data collated from both sources as well as a visualisation dashboard. The below figure details the current workflow of the system. 

![lambdaArch](https://github.com/aanchal-n/Lambda-Architecture-AWS/blob/main/Assets/aws-lambda-architecture.png)

### Batch Layer 


# Installation and Running 
# Usage
# Test Scripts

