package com.example.kafkaanddbtransaction;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AnswerWaitor<T> implements Answer<T> {

    private CountDownLatch countDownLatch = new CountDownLatch(2);

    @Override
    public T answer(InvocationOnMock invocationOnMock) throws Throwable {

        try {
            T result = ((T) invocationOnMock.callRealMethod());
            return result;
        } finally {
            countDownLatch.countDown();
        }
    }

    public void await() throws InterruptedException {
        countDownLatch.await(10L, TimeUnit.SECONDS);
//        return this::answer;
    }


}
