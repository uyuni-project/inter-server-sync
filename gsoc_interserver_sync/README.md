# Project
Provide a graphical interface for the Inter-Server Sync v2 (Google Summer of Code 2023)    
Owner/Contributor: [Aniket Mishra](https://github.com/iAniket23) (ianiket23@gmail.com).   
Mentor: [Artem Shiliaev](https://github.com/avshiliaev)

# Overview 
The objective of this project is to provide a graphical user interface (GUI) for the Inter-Server Sync v2 feature of SUSE Manager. The project will be divided into two main parts.

First, we will transform the existing Inter-Server Sync command-line interface into a service unit daemon that runs at recurring time intervals and provides an API for the GUI. We will make it with Golang using Gin and Gorm.
The second part of the project will involve building a basic React-based app that is used to do data visualization as well as able to communicate with the API to trigger synchronization.

# Pre Proposal Work 
In this pre proposal work, the following things are implemented to get started on the Project.
  1. React App (Typescript)
  2. State Management is implemented using Redux (Increment button below shows this implementation)
  3. Backend in Golang is setup
  4. API is implemented in Golang using Gin which generates a random number (Clicking on the "API call" button to generate random number shows this       implementation)   

An React app with Redux (Typescript) is implemented which makes an API call to our backend which is made in Golang. The backend Golang provides an API which generates a random number and the React app displays the number on the screen.

# Running the code
## Backend  
It is a Golang backend (using Gin) which returns a random number when (/random) endpoint is hit
```
go get .
go run .
```
## Frontend  
It is a React Redux App (Typescript)
```
cd gsoc_interserver_sync_gui
npm install
npm run start
```
