package com.hmdp.ops;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class OpsSecurityTest {
    private MockMvc mvc;
    private OpsReadService service;
    private OpsSecurityFilter filter;
    private static final String SECRET="only-test-admin-token-with-at-least-32-characters";
    @BeforeEach void setup() {
        service=mock(OpsReadService.class);
        when(service.read(anyString(),anyLong(),anyLong(),anyLong(),anyString())).thenReturn(Collections.singletonMap("readOnly",true));
        OpsController controller=new OpsController();ReflectionTestUtils.setField(controller,"service",service);
        filter=new OpsSecurityFilter();ReflectionTestUtils.setField(filter,"adminToken",SECRET);
        ReflectionTestUtils.setField(filter,"enabled",true);
        ReflectionTestUtils.setField(filter,"journal",new OpsJournal());
        mvc=MockMvcBuilders.standaloneSetup(controller).addFilter(filter).build();
    }
    @Test void shouldDisableDiagnosticsByDefaultEvenWithValidCredentials() throws Exception {
        ReflectionTestUtils.setField(filter,"enabled",false);
        mvc.perform(get("/internal/ops/inventory").header("X-Ops-Token",SECRET)).andExpect(status().isNotFound());
        verifyNoInteractions(service);
    }
    @Test void shouldRejectConsumerAuthenticationAndMissingAdminSecret() throws Exception {
        mvc.perform(get("/internal/ops/inventory").header("authorization","ordinary-user")).andExpect(status().isForbidden());
        verifyNoInteractions(service);
        assertFalse(OpsSecurityFilter.authorized("", ""));
        assertFalse(OpsSecurityFilter.authorized("short", "short"));
        assertFalse(OpsSecurityFilter.authorized(SECRET,"wrong"));
        assertTrue(OpsSecurityFilter.authorized(SECRET,SECRET));
    }
    @Test void shouldForbidWriteMethodsEvenForAdministrator() throws Exception {
        mvc.perform(post("/internal/ops/inventory").header("X-Ops-Token",SECRET)).andExpect(status().isMethodNotAllowed());
        verifyNoInteractions(service);
    }
    @Test void shouldReadOnlyWithBoundedTypedScope() throws Exception {
        long now=System.currentTimeMillis();
        mvc.perform(get("/internal/ops/inventory").header("X-Ops-Token",SECRET)
                .param("start",String.valueOf(now-1000)).param("end",String.valueOf(now)).param("voucherId","95001"))
                .andExpect(status().isOk()).andExpect(header().string("Cache-Control","no-store"))
                .andExpect(jsonPath("$.data.readOnly").value(true));
        verify(service).read(eq("inventory"),anyLong(),anyLong(),eq(95001L),eq("127.0.0.1"));
    }
    @Test void shouldRejectArbitraryKeysAndOverlongWindowsBeforeReading() throws Exception {
        long now=System.currentTimeMillis();
        mvc.perform(get("/internal/ops/inventory").header("X-Ops-Token",SECRET)
                .param("start",String.valueOf(now-1000000)).param("end",String.valueOf(now)).param("voucherId","95001"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/internal/ops/inventory").header("X-Ops-Token",SECRET)
                .param("start",String.valueOf(now-1000)).param("end",String.valueOf(now)).param("voucherId","1 OR 1=1"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }
}
