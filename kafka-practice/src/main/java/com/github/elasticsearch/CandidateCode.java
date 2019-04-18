package com.github.elasticsearch;

import java.io.*;
import java.util.*;

public class CandidateCode {

    public static void main(String[] args) throws IOException {
        new CandidateCode().run();
    }

    private void run() throws IOException {
        Scanner in = new Scanner(System.in);
        int numOfTests = in.nextInt();
        for(int i=0;i<numOfTests;i++) {
            Integer numOfPlayers = in.nextInt();
            List<CandidateCodeObject> objectList =
                    new ArrayList<CandidateCodeObject>();
            for (int l = 0; l < numOfPlayers; l++) {
                CandidateCodeObject obj = new CandidateCodeObject(
                        in.nextInt(), "v");
                objectList.add(obj);
            }
            for (int j = 0; j < numOfPlayers; j++) {
                CandidateCodeObject obj1 = new CandidateCodeObject
                        (in.nextInt(), "p");
                objectList.add(obj1);
            }



            Comparator<CandidateCodeObject> byEnergyValue =
                    (CandidateCodeObject o1, CandidateCodeObject o2) ->
                            o1.getEnergyValue().compareTo(o2.getEnergyValue());
            objectList.sort(byEnergyValue);

            boolean hasWon = true;
            for (CandidateCodeObject obj : objectList) {
           //     System.out.println(obj.toString());
            }
            int numV = 0;
            for (int k = 0; k < 2 * numOfPlayers; k++) {
                String p = objectList.get(k).getType();
                if (p.equals("v"))
                    numV += 1;
                else
                    numV -= 1;
                if (numV == -1) {
                    hasWon = false;
                    break;
                }
            }
            if (hasWon)
                System.out.println("WIN");
            else
                System.out.println("LOSE");
        }

    }



    private class CandidateCodeObject {
        private Integer energyValue;
        private String type;

        public CandidateCodeObject(Integer energyValue, String type) {
            this.energyValue = energyValue;
            this.type = type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CandidateCodeObject)) return false;

            CandidateCodeObject that = (CandidateCodeObject) o;

            if (!energyValue.equals(that.energyValue)) return false;
            return type.equals(that.type);
        }

        public Integer getEnergyValue() {
            return energyValue;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return "CandidateCodeObject{" +
                    "energyValue=" + energyValue +
                    ", type='" + type + '\'' +
                    '}';
        }
    }


}

