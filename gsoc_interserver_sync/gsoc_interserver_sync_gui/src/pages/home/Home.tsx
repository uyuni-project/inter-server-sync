import "./Home.css";
import React from "react";

export default function Home() {
    return (
        <div className="Home">
            <h1 className="Heading">OpenSUSE <i className = "HeadingOne">GSoC 2023 (Pre Proposal Work)</i></h1>
            <h2 className="subHeading"> Project: <i className = "HeadingOne">Provide a graphical interface for the Inter-Server Sync v2</i></h2>
            <p>Owner/Contributor: Aniket Mishra (<a href="mailto:ianiket23@gmail.com">ianiket23@gmail.com</a>)</p>
            <p>Mentor: Artem Shiliaev</p>
            <p> The objective of this project is to provide a graphical user interface (GUI) for the <b>Inter-Server Sync v2</b> feature of SUSE Manager.
            The project will be divided into two main parts.
            </p>
            <ul>
                <li>First, we will transform the existing Inter-Server Sync <b>command-line interface</b> into a service
                    <b> unit daemon</b> that runs at recurring time intervals and provides an API for the GUI. We will make it with <b>Golang using Gin and Gorm</b>.
                </li>
                <li>The second part of the project will involve building a basic <b>React-based app</b> that is used to do <b>data visualization</b> as well as able
                    to communicate with the API to trigger synchronization.
                </li>
            </ul>
            <hr></hr>
            <h2 className="subHeadingTwo"> Pre Proposal Work: </h2>
            <p><b>In this pre proposal work</b>, the <b>following things are implemented</b> to get started on the Project.</p>
            <ul>
                <li>React App (Typescript)</li>
                <li><b>State Management</b> is implemented using Redux (<i>Increment button below shows this implementation</i>)</li>
                <li>Backend in Golang is setup</li>
                <li><b>API is implemented in Golang</b> using Gin which generates a random number (<i>Clicking on the "API call" button to generate random number shows this implementation</i>)</li>
            </ul>
            <p>
                An React app with Redux (Typescript) is implemented which makes an API call to our backend which is made in Golang.
                The backend Golang provides an API which generates a random number and the React app displays the number on the screen.
            </p>
        </div>
    );
}