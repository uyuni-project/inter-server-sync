import React from 'react';
import Home from './pages/home/Home';
import './App.css';
import {Counter} from "./pages/components/counter/Counter";
import {Random} from "./pages/components/random/Random";

function App() {

  return (
    <div>
      <Home />
        <div className='components'>
          <Counter />
          <Random />
        </div>
    </div>
  );
}

export default App;
