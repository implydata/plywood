// Type definitions for MySQL
//
// Author: Vadim Ogievetsky

declare module MySQL {
  export interface DescribeResult {
    Field: string;
    Type: string;
  }
}