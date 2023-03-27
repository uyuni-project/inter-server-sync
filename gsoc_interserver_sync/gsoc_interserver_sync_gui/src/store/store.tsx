import { configureStore } from "@reduxjs/toolkit";
import counterReducer from "../pages/components/counter/counterSlice";
import randomReducer from "../pages/components/random/randomSlice";

export default configureStore({
    reducer: {
      counter: counterReducer,
      random: randomReducer,
    },
  });