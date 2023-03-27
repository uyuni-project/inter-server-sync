import { createSlice } from '@reduxjs/toolkit';

interface RandomState {
    value: number;
}

const initialState: RandomState = {
    value: 0,
};

export const randomSlice = createSlice({
    name: 'random',
    initialState,
    reducers: {
        randomValue: (state, action) => {
            state.value = action.payload;
        },
    },
});

export const { randomValue } = randomSlice.actions;

export const selectRandom = (state: { random: RandomState }) => state.random.value;

export default randomSlice.reducer;