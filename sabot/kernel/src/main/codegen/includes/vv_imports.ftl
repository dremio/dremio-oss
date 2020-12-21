<#--

    Copyright (C) 2017-2019 Dremio Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.common.base.Charsets;
import com.google.common.collect.ObjectArrays;

import com.google.common.base.Preconditions;
import io.netty.buffer.NettyArrowBuf;

import org.apache.commons.lang3.ArrayUtils;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.expr.fn.impl.StringFunctionUtil;
import org.apache.arrow.memory.*;
import com.dremio.exec.proto.SchemaDefProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.*;
import com.dremio.common.exceptions.*;
import com.dremio.exec.exception.*;
import com.dremio.common.expression.FieldReference;
import org.apache.arrow.vector.util.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.*;
import org.apache.arrow.vector.holders.*;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.common.util.DremioStringUtils;
import com.dremio.exec.vector.*;
import com.dremio.exec.vector.complex.*;

import org.apache.arrow.memory.OutOfMemoryException;

import com.sun.codemodel.JType;
import com.sun.codemodel.JCodeModel;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Random;
import java.util.List;

import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.LocalDateTime;
import org.joda.time.Period;

import com.dremio.exec.vector.accessor.sql.TimePrintMillis;
import javax.inject.Inject;





