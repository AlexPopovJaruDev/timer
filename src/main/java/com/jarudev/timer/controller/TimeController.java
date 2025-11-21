package com.jarudev.timer.controller;

import com.jarudev.timer.service.TimeService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequiredArgsConstructor
public class TimeController {

    private final TimeService timeService;

    @GetMapping("/api/times")
    public List<LocalDateTime> getAllTimeEntries() {
        return timeService.findAll();
    }
}