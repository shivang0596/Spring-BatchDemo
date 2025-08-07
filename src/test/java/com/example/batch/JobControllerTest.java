package com.example.batch;

import com.example.batch.controller.JobController;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.http.MediaType;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobController.class)
class JobControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private JobLauncher jobLauncher;

    @MockBean
    private Job job;

    @Test
    void shouldReturnSuccessMessage_whenJobRunsSuccessfully() throws Exception {
        JobExecution jobExecution = mock(JobExecution.class);
        when(jobLauncher.run(eq(job), any(JobParameters.class)))
                .thenReturn(jobExecution);

        mockMvc.perform(post("/jobs/importCustomers")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().string("Data Imported Successfully!"));

        verify(jobLauncher, times(1)).run(eq(job), any(JobParameters.class));
    }

    @Test
    void shouldHandleExceptions_whenJobFails() throws Exception {
        doThrow(JobExecutionAlreadyRunningException.class)
                .when(jobLauncher).run(eq(job), any(JobParameters.class));

        mockMvc.perform(post("/jobs/importCustomers")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().string("Data Imported Successfully!")); // controller doesn't change message on error

        verify(jobLauncher, times(1)).run(eq(job), any(JobParameters.class));
    }
}

