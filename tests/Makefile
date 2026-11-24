PLUGIN ?= ../libnccl-net-mrc.so
ROOT := $(abspath ..)
MRC_HOME ?= /opt/mellanox/doca
MRC_LIBDIR ?= $(MRC_HOME)/lib/aarch64-linux-gnu
CUDA_HOME ?= /usr/local/cuda
OBJDIR ?= $(ROOT)/build/obj
INIT_TRACE_DIR ?= $(ROOT)/support
INIT_TRACE_LIBDIR ?= $(abspath $(dir $(PLUGIN)))
PYTHON ?= python3
CXX ?= g++

CPPFLAGS += -DNCCL_OS_LINUX -DNCCL_BUILD_RDMA_CORE -DNCCL_COMM_H_ -DncclNetIb=ncclNetPlugin_v12
CPPFLAGS += -I$(ROOT)/src -I$(ROOT)/src/include -I$(ROOT)/src/include/plugin
CPPFLAGS += -I$(INIT_TRACE_DIR)/src -I$(MRC_HOME)/include
CPPFLAGS += -I$(CUDA_HOME)/include -I$(CUDA_HOME)/include/cccl -I$(OBJDIR)/include
ifneq ($(NCCL_HOME),)
CPPFLAGS += -I$(NCCL_HOME)/include
endif
CXXFLAGS := -std=c++17 -O1 -g -pthread -Wall -Wextra -Wno-unused-parameter -Wno-sign-compare
LDFLAGS += -L$(INIT_TRACE_LIBDIR) -Wl,-rpath,$(INIT_TRACE_LIBDIR)
LDFLAGS += -L$(MRC_LIBDIR) -Wl,-rpath,$(MRC_LIBDIR)
LDFLAGS += -L$(CUDA_HOME)/lib64 -Wl,-rpath,$(CUDA_HOME)/lib64
LDLIBS += -lnccl-init-trace -libverbs -lnv_mrc -lcudart -ldl
OBJECTS = $(shell find $(OBJDIR) -name '*.o' ! -path '*/transport/net_ib/connect.o')
FIXTURE := $(ROOT)/build/early_mrc_fixture

test: $(FIXTURE)
	NCCL_IB_QPS_PER_CONNECTION=2 $(FIXTURE)
	NCCL_IB_QPS_PER_CONNECTION=2 NCCL_IB_PREPOST_RECEIVE_WORK_REQUESTS=1 NCCL_IB_USE_INLINE=1 $(FIXTURE)
	NCCL_INIT_TRACE_LIBRARY=$(INIT_TRACE_LIBDIR)/libnccl-init-trace.so \
	    $(PYTHON) -m unittest discover -s . -p 'test_*.py' -v

$(FIXTURE): early_mrc_fixture.cc $(ROOT)/src/transport/net_ib/connect.cc $(OBJECTS) $(INIT_TRACE_LIBDIR)/libnccl-init-trace.so
	@mkdir -p $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(OBJECTS) $(LDFLAGS) $(LDLIBS) -o $@

.PHONY: test
