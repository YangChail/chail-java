//给定一个 m x n 二维字符网格 board 和一个字符串单词 word 。如果 word 存在于网格中，返回 true ；否则，返回 false 。 
//
// 单词必须按照字母顺序，通过相邻的单元格内的字母构成，其中“相邻”单元格是那些水平相邻或垂直相邻的单元格。同一个单元格内的字母不允许被重复使用。 
//
// 
//
// 例如，在下面的 3×4 的矩阵中包含单词 "ABCCED"（单词中的字母已标出）。 
//
// 
//
// 
//
// 示例 1： 
//
// 
//输入：board = [["A","B","C","E"],["S","F","C","S"],["A","D","E","E"]], word = 
//"ABCCED"
//输出：true
// 
//
// 示例 2： 
//
// 
//输入：board = [["a","b"],["c","d"]], word = "abcd"
//输出：false
// 
//
// 
//
// 提示： 
//
// 
// m == board.length 
// n = board[i].length 
// 1 <= m, n <= 6 
// 1 <= word.length <= 15 
// board 和 word 仅由大小写英文字母组成 
// 
//
// 注意：本题与主站 79 题相同：https://leetcode-cn.com/problems/word-search/ 
//
// Related Topics 数组 回溯 矩阵 👍 761 👎 0


//leetcode submit region begin(Prohibit modification and deletion)
class Solution {
    public static boolean exist(char[][] board, String word) {
        int length = word.length();
        int i;
        int k;
        int aa = 0;
        for (int f = 0; f < word.length(); f++) {
            for (i = 0; i < board.length; i++) {
                for (k = 0; k < board[i].length; k++) {
                    char c = board[i][k];
                    if (c == word.charAt(f)) {
                        if (k + 1 >= board[i].length || i + 1 >= board.length) {
                            break;
                        }
                        char c_1 = board[i + 1][k];
                        char c_2 = board[i][k + 1];
                        if (c_1 == word.charAt(f + 1)) {
                            i++;
                        } else if (word.charAt(f + 1) == c_2) {
                            k++;
                        }
                        f++;
                        aa++;
                    }
                }
            }
        }
        if (aa == length) {
            return true;
        }
        return false;
    }


    public static void vv(){




    }




    public static void main(String[] args) {
        char[][] board = {{'A','B','C','E'},{'S','F','C','S'},{'A','D','E','E'}};
        String word = "ABCCED";
        exist(board,word);


    }
}
//leetcode submit region end(Prohibit modification and deletion)
