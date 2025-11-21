package com.jarudev.timer;

import org.springframework.boot.SpringApplication;

public class TestTimerApplication {

    public static void main(String[] args) {
        SpringApplication.from(TimerApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
