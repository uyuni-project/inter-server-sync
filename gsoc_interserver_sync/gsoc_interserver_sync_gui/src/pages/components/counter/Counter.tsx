import React from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { increment, selectCount } from './counterSlice';
import "./Counter.css";

export function Counter() {
    const count = useSelector(selectCount);
    const dispatch = useDispatch();
    return (
        <div className='counterElement'>
        <div>
            <button className = "incrementButton" aria-label="Increment value" onClick={() => dispatch(increment())}>
            Increment
            </button>
        </div>
        <div>Count: {count}</div>
        </div>
    );
}