/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.test.schemata.bookclub;

public final class BookClubSchema {

  public final BookClubMember[] MEMBERS = {
      new BookClubMember(111, "{\"Name\":\"John Smith\",\"address\":{\"streetAddress\":\"21 2nd " +
          "Street\",\"city\":\"New York\",\"state\":\"NY\",\"postalCode\":10021}," +
          "\"phoneNumber\":[{\"type\":\"home\",\"number\":\"212 555-1234\"},{\"type\":\"fax\"," +
          "\"number\":\"646 555-4567\"}],\"books\":[{\"title\":\"The Talisman\"," +
          "\"authorList\":[\"Stephen King\",\"Peter Straub\"],\"category\":[\"SciFi\"," +
          "\"Novel\"]},{\"title\":\"Far from the Madding Crowd\",\"authorList\":[\"Thomas " +
          "Hardy\"],\"category\":[\"Novel\"]}]}"),
      new BookClubMember(222, "{\"Name\":\"Peter Walker\",\"address\":{\"streetAddress\":\"111 " +
          "Main Street\",\"city\":\"San Jose\",\"state\":\"CA\",\"postalCode\":95111}," +
          "\"phoneNumber\":[{\"type\":\"home\",\"number\":\"408 555-9876\"},{\"type\":\"office\"," +
          "\"number\":\"650 555-2468\"}],\"books\":[{\"title\":\"Good Omens\"," +
          "\"authorList\":[\"Neil Gaiman\",\"Terry Pratchett\"],\"category\":[\"Fantasy\"," +
          "\"Novel\"]},{\"title\":\"Smoke and Mirrors\",\"authorList\":[\"Neil Gaiman\"]," +
          "\"category\":[\"Novel\"]}]}"),
      new BookClubMember(333, "{\"Name\":\"James Lee\"}")
  };

  public static class BookClubMember {
    public final int ID;
    public final String JCOL;


    public BookClubMember(int id, String jcol) {
      this.ID = id;
      this.JCOL = jcol;
    }
  }
}
