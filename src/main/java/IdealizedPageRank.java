import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class IdealizedPageRank {
    //groups with () are useful, i love them
    private static final Pattern links = Pattern.compile("^(\\d+):(.*)$");
    //ill figure out titles later i dont know what I'm looking at
    //private static final Pattern titles

    public static void main(String[] args) {
        String input = args[0];
        String output = args[1];

        Matcher linkMatcher = links.matcher(input);

        HashMap<String, String[]> linkMap = new HashMap<>();
        //look this isnt the right implementation in the slightest but its a placeholder to show how the regex works
        while (linkMatcher.find()) {
            linkMap.put(linkMatcher.group(1), linkMatcher.group(2).split(" "));
        }

    }
}
