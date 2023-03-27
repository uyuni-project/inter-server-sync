import React from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { randomValue, selectRandom } from './randomSlice';
import './Random.css';
import axios from 'axios';

export function Random() {
    const random = useSelector(selectRandom);
    const dispatch = useDispatch();

    const handleApiCall = () => {
        axios.get('http://localhost:8080/random')
            .then((response) => {
                const randomNumber = response.data;
                console.log(response);
                dispatch(randomValue(randomNumber));
            })
            .catch((error) => {
                console.log(error);
            });
    };

    return (
        <div className='randomElement'>
            <div>
                <button className='randomButton' onClick={handleApiCall}>API Call</button>
            </div>
            <div>Random number: {random} </div>
        </div>
    );
}