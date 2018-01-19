/**
 *     Copyright 2018 The Jarasandha.io project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jarasandha.store.filesystem;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by ashwin.jayaprakash.
 */
public abstract class JsonSampleCreator {
    private static final String[] SAMPLE_NAMES = {
            "Adriana C. Ocampo Uria",
            "Albert Einstein",
            "Anna K. Behrensmeyer",
            "Blaise Pascal",
            "Caroline Herschel",
            "Cecilia Payne-Gaposchkin",
            "Chien-Shiung Wu",
            "Dorothy Hodgkin",
            "Edmond Halley",
            "Edwin Powell Hubble",
            "Elizabeth Blackburn",
            "Enrico Fermi",
            "Erwin Schroedinger",
            "Flossie Wong-Staal",
            "Frieda Robscheit-Robbins",
            "Geraldine Seydoux",
            "Gertrude B. Elion",
            "Ingrid Daubechies",
            "Jacqueline K. Barton",
            "Jane Goodall",
            "Jocelyn Bell Burnell",
            "Johannes Kepler",
            "Lene Vestergaard Hau",
            "Lise Meitner",
            "Lord Kelvin",
            "Maria Mitchell",
            "Marie Curie",
            "Max Born",
            "Max Planck",
            "Melissa Franklin",
            "Chelsey Cranford",
            "Muriel Musgrove",
            "Cathi Ceron",
            "Zella Zynda",
            "Willodean Wacker",
            "Pam Powell",
            "Jovan Jin",
            "Almeta Apple",
            "Elwood Eargle",
            "Harland Hornback",
            "Pauline Penna",
            "Christin Calhoon",
            "Tamera Trevino",
            "Ling Landau",
            "Kory Kuss",
            "Babette Buie",
            "Senaida Schmitz",
            "Ara Ashley",
            "Virgil Viars",
            "Latia Legler",
            "Michael Faraday",
            "Mildred S. Dresselhaus",
            "Nicolaus Copernicus",
            "Niels Bohr",
            "Patricia S. Goldman-Rakic",
            "Patty Jo Watson",
            "Polly Matzinger",
            "Richard Phillips Feynman",
            "Rita Levi-Montalcini",
            "Rosalind Franklin",
            "Ruzena Bajcsy",
            "Sarah Boysen",
            "Shannon W. Lucid",
            "Shirley Ann Jackson",
            "Sir Ernest Rutherford",
            "Sir Isaac Newton",
            "Stephen Hawking",
            "Werner Karl Heisenberg",
            "Wilhelm Conrad Roentgen",
            "Wolfgang Ernst Pauli"
    };
    private static final String[] SAMPLE_CITIES = {"San Jose", "Bangalore", "Tokyo", "Singapore"};
    private static final String[] SAMPLE_INVESTMENT_COMPANIES = {"Fidelity", "WellsFargo", "Etrade", "Schwab"};

    private JsonSampleCreator() {
    }

    public static Record newRecord(Random tlsRandom) {
        final Record record = new Record();
        record.name = randomPick(tlsRandom, SAMPLE_NAMES);
        record.age = tlsRandom.nextInt(100);
        record.friends = new HashSet<>(4);

        for (int j = 0; j < 24; j++) {
            final Friend friend = new Friend();
            friend.name = randomPick(tlsRandom, SAMPLE_NAMES);
            friend.recentTrips = new String[2];
            for (int k = 0; k < friend.recentTrips.length; k++) {
                friend.recentTrips[k] = randomPick(tlsRandom, SAMPLE_CITIES);
            }
            record.friends.add(friend);
        }

        record.checking = new Account();
        record.checking.company = randomPick(tlsRandom, SAMPLE_INVESTMENT_COMPANIES);
        record.checking.balance = roughRoundOff(1000.0 * tlsRandom.nextDouble());

        record.saving = new Account();
        record.saving.company = randomPick(tlsRandom, SAMPLE_INVESTMENT_COMPANIES);
        record.saving.balance = roughRoundOff(2200.0 * tlsRandom.nextDouble());

        record.retirement = new Account();
        record.retirement.company = randomPick(tlsRandom, SAMPLE_INVESTMENT_COMPANIES);
        record.retirement.balance = roughRoundOff(3400.0 * tlsRandom.nextDouble());
        return record;
    }

    private static String randomPick(Random tlRandom, String[] items) {
        return items[tlRandom.nextInt(items.length)];
    }

    private static double roughRoundOff(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    @EqualsAndHashCode(of = {"name", "age"})
    @Getter
    @Setter
    public static class Record {
        String name;
        int age;
        Set<Friend> friends;
        Account checking;
        Account saving;
        Account retirement;
    }

    @EqualsAndHashCode(of = "name")
    @Getter
    @Setter
    public static class Friend {
        String name;
        String[] recentTrips;
    }

    @EqualsAndHashCode(of = "company")
    @Getter
    @Setter
    public static class Account {
        String company;
        double balance;
    }
}
