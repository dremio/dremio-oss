package com.dremio.exec.compile;

public class BarTest {
    public BarTest() {
    }

    public void success() {
        com.dremio.exec.compile.FooTest fooTest = new com.dremio.exec.compile.FooTest();
        fooTest.success();
    }
}
